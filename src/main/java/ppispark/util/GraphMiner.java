package ppispark.util;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import ppiscala.graphUtil;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;

public class GraphMiner {
    public static GraphFrame apply(PPInetwork ppi,SparkSession spark,int functionIndex,String inputNode,int weightIndex){

        GraphFrame output=ppi.getGraph();
        switch (functionIndex){
            case 1: //F4
                ArrayList<Object> N=new ArrayList<Object>();
                N.add(inputNode);
                output=xNeighborsGraph(output,N,2);//(output,N,functionArgs[1]) // N e x
                break;
            case 2: //F6
                output=xNeighborsWeightedGraph(output,spark,"uniprotkb:P51584",3,weightIndex);  //Spark inputnode x
                break;
            case 3: //F7
               //output=xNeighborsWeightedGraph(output);//GraphFrame graph,SparkSession spark,ArrayList<Object> input, int x
                break;
            case 4:
               //output=neighborsWeightedGraph(output); //GraphFrame graph,SparkSession spark,ArrayList<ArrayList<Object>> input, int x
                break;
            case 5:
                //output=weightedSubgraphWithLabels(output); //GraphFrame graph,SparkSession spark,ArrayList<ArrayList<Object>> input, int x
                break;
        }
        return output;
    }

    //F4
    public static  GraphFrame xNeighborsGraph (GraphFrame graph,ArrayList<Object> N, int x, boolean KEEP){
        Dataset<Row> paths=graph.shortestPaths().landmarks(N).run();
        Dataset<Row> explodedPaths=paths
                .select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")))
                .filter("value<="+x)
                .drop("value")
                .groupBy("id")
                .agg(org.apache.spark.sql.functions.collect_list("key").as("neighbors"));
        Dataset<Row> edges=graph.edges()
                .join(explodedPaths,graph.edges().col("src").equalTo(explodedPaths.col("id")));
        edges=edges.drop("id","neighbors");
        edges=edges.join(explodedPaths,edges.col("dst").equalTo(explodedPaths.col("id")));
        edges=edges.drop("id","neighbors");


        Dataset<Row> vertices=graph.vertices().join(explodedPaths,"id");

        if(KEEP){
            return GraphFrame.apply(vertices,edges);
        }else{
            return GraphFrame.fromEdges(edges);
        }
    }

    public static GraphFrame xNeighborsGraph (GraphFrame graph,ArrayList<Object> N, int x){
        return xNeighborsGraph(graph,N,x,true);
    }
    //F1
    public static  Dataset<Row> findMaxComponent(GraphFrame graph,ArrayList<Object> N, int x){
        if(x > 0){
            graph=xNeighborsGraph(graph,N,x,false);
        }
        Dataset<Row> components=graph.connectedComponents().run();
        Tuple2<Long, Integer> max=components.javaRDD()
                .mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
                .mapToPair(new Ncounter2(N))
                .reduceByKey((i1,i2)->{return i1+i2;})
                .max(new comparator());
        Dataset<Row> maxComponent=components.filter("component="+max._1);

        return maxComponent;
    }

    public static Dataset<Row> findMaxComponent(GraphFrame graph,ArrayList<Object> N){
        return findMaxComponent(graph,N,0);
    }
    //F2
    public static Dataset<Row> componentsIntersection(GraphFrame graph, SparkSession spark,ArrayList<Object> N, int x) throws IOException {
        if(x>0) {
            graph=xNeighborsGraph(graph,N,x,false);
        }
        Dataset<Row> components=graph.connectedComponents().run();


        JavaRDD<Row> intersections=components.javaRDD()
                .mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
                .mapToPair(new Ncounter2(N))
                .reduceByKey((i1,i2)->{return i1+i2;})
                .map(t->{Row r= RowFactory.create(t._1.toString(),t._2);return r;});


        StructType schemaVertices=new StructType()
                .add("component-id","String")
                .add("|intersection|","Integer");

        Dataset<Row> output=spark.createDataFrame(intersections,schemaVertices);
        return output;
    }

    public static Dataset<Row> componentsIntersection(GraphFrame graph,SparkSession spark,ArrayList<Object> N) throws IOException {
        return componentsIntersection(graph,spark,N,0);
    }

    //F3
    public static Dataset<Row> xNeighbors(GraphFrame graph,String id, int x){
        ArrayList<Object> landmarks=new ArrayList<Object>();
        landmarks.add(id);
        Dataset<Row> shortestPaths=graph.shortestPaths().landmarks(landmarks).run();
        Dataset<Row> output=shortestPaths
                .select(shortestPaths.col("id"),org.apache.spark.sql.functions.explode(shortestPaths.col("distances")))
                .filter("value<="+x)
                .drop("key")
                .drop("value");
        return output;
    }

    //F5
    public static Dataset<Row> xWeightedNeighbors(GraphFrame graph,SparkSession spark,String inputNode, int x,int weightIndex){

        Dataset<Row> edges=graph.edges().withColumn("weight", org.apache.spark.sql.functions.lit(-1));
        GraphFrame graph1=GraphFrame.fromEdges(edges);
        graph1.edges().show();
        Dataset<Row> weightedPath= graphUtil.maxWeightedPaths(graph1,inputNode,spark,weightIndex);
        return weightedPath.filter("weight>="+x+" OR weight==0");
    }

    //F6
    public static GraphFrame xNeighborsWeightedGraph(GraphFrame graph,SparkSession spark,String inputNode, int x,int weightIndex){
        Dataset<Row> xNeighbors=xWeightedNeighbors(graph,spark,inputNode,x,weightIndex);
        Dataset<Row> edges=graph.edges();
        edges=edges.join(xNeighbors,edges.col("src").equalTo(xNeighbors.col("id")));
        edges=edges.drop("id").drop("weight");
        edges=edges.join(xNeighbors,edges.col("dst").equalTo(xNeighbors.col("id")));
        edges=edges.drop("id").drop("weight");
        return GraphFrame.fromEdges(edges);
    }

    //F7
    public static GraphFrame xNeighborsWeightedGraph(GraphFrame graph,SparkSession spark,ArrayList<Object> input, int x){
        Dataset<Row> edges=graph.edges().withColumn("weight", org.apache.spark.sql.functions.lit(-1));
        GraphFrame graph1=GraphFrame.fromEdges(edges);
        Dataset<Row> xNeighbors=graphUtil.maxWeightedPaths(graph1,input,spark);
        Dataset<Row> vertices=xNeighbors.filter("weight>="+x+" OR weight==0")
                .groupBy("id")
                .agg(org.apache.spark.sql.functions.collect_list("key").as("key"));
        edges=graph1.edges();
        edges=edges.join(vertices,edges.col("src").equalTo(vertices.col("id")));
        edges=edges.drop("id").drop("key");
        edges=edges.join(vertices,edges.col("dst").equalTo(vertices.col("id")));
        edges=edges.drop("id").drop("key");
        GraphFrame output=GraphFrame.apply(vertices,edges);
        return output;
    }

    //F8
    public static GraphFrame neighborsWeightedGraph(GraphFrame graph,SparkSession spark,ArrayList<ArrayList<Object>> input, int x){
        //List<String> data = Arrays.asList("A","B","C","D","E");
        SparkContext sparkContext=new SparkContext();
        Dataset<Row> edges=graph.edges().withColumn("weight", org.apache.spark.sql.functions.lit(-1));
        GraphFrame graph1=GraphFrame.fromEdges(edges);
        StructType s = new StructType()
                .add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("key", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("weight", DataTypes.DoubleType, true, Metadata.empty()));

        Dataset<Row> xNeighbors = spark.read().schema(s).csv(spark.emptyDataset(Encoders.STRING()));

        for(int i=0;i< input.size();i++){
            Dataset<Row> tmp=graphUtil.maxWeightedPaths(graph1, input.get(i),i,spark);
            xNeighbors=xNeighbors.union(tmp);
        }
        Dataset<Row> vertices=xNeighbors.filter("weight>="+x+" OR weight==0")
                .groupBy("id")
                .agg(org.apache.spark.sql.functions.collect_list("key").as("key"));
        edges=graph1.edges();
        edges=edges.join(vertices,edges.col("src").equalTo(vertices.col("id")));
        edges=edges.drop("id").drop("key");
        edges=edges.join(vertices,edges.col("dst").equalTo(vertices.col("id")));
        edges=edges.drop("id").drop("key");
        GraphFrame output=GraphFrame.apply(vertices,edges);
        return output;

    }

    //F9
    public static GraphFrame weightedSubgraphWithLabels(GraphFrame graph,SparkSession spark,ArrayList<ArrayList<Object>> input, int x){

        JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
        JavaRDD<Row> N=jsc.emptyRDD();


        for(int i=0;i<input.size();i++){
            int y=i+1;
            JavaRDD<Row> tmp=jsc.parallelize(input.get(i)).map(t->{
                Object[] o=new Object[]{t, y};
                Row r=RowFactory.create(o);
                return r;
            });
            N=N.union(tmp);
        }

        StructType s = new StructType()
                .add(new StructField("id1", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("N", DataTypes.IntegerType, true, Metadata.empty()));

        Dataset<Row> prova=spark.createDataFrame(N, s).groupBy("id1")
                .agg(org.apache.spark.sql.functions.collect_list("N").as("N"));;


        GraphFrame g=xNeighborsWeightedGraph(graph,spark,input.get(0),x);
        Dataset<Row> vertices=g.vertices();
        vertices=vertices.join(prova,vertices.col("id")
                .equalTo(prova.col("id1")),"left")
                .drop("id1");
        return GraphFrame.apply(vertices,g.edges());
    }

}
