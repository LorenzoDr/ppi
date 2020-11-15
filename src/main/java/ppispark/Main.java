package ppispark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;


import java.io.File;
import java.io.Serializable;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        boolean local = true;

        String path = local ? "data/human_small.tsv" : args[0];
        // path = local ? "data/ridotto.tsv" : args[0];

        SparkSession spark;

        if (local){
            System.setProperty("spark.sql.legacy.allowUntypedScalaUDF", "true");
            spark = SparkSession
                    .builder()
                    .master("local[*]")
                    .appName("biograph")
                    .getOrCreate();
        }
        else
            spark = SparkSession
                    .builder()
                    .master("yarn")
                    .appName("biograph")
                    .config("spark.executor.instances", "8")
                    .config("spark.executor.cores", "6")
                    .config("spark.executor.memory", "20g")
                    .config("spark.debug.maxToStringFields", "50")
                    .getOrCreate();

        Dataset<Row> edges = spark.read().
                option("header", "True").
                option("delimiter", "\t").
                csv(path);

        String[] cols_renames = new String[]{
                "src", "dst", "alt_id_A", "alt_id_B", "alias_A", "alias_B", "det_method", "first_auth", "id_pub", "ncbi_id_A", "ncbi_id_B",
                "int_types", "source_db", "int_id", "conf_score", "comp_exp", "bio_role_A", "bio_role_B", "exp_role_A", "exp_role_B", "type_A", "type_B",
                "xref_A", "xref_B", "xref_int", "annot_A", "annot_B", "annot_int", "ncbi_id_organism", "param_int", "create_data", "up_date",
                "chk_A", "chk_B", "chk_int", "negative", "feat_A", "feat_B", "stoich_A", "stoich_B", "part_meth_A", "part_meth_B"};

        edges = edges.toDF(cols_renames);

        GraphFrame graph = GraphFrame.fromEdges(edges);

        spark.sparkContext().setCheckpointDir(local ? "checkpoint" : "hdfs://master.local:8020/user/hduser/data/checkpoint");

        Row[] rows = (Row[]) graph.vertices().head(100);

        ArrayList<Object> N = new ArrayList<>();


        for (Row row : rows)
            N.add(row.getString(0));

        System.out.println("Number of vertices of the connected component maximizing nodes in N: " + F1(graph, N, 2).count());

        for (Map.Entry<Dataset<Row>, Integer> el : F2(graph, N).entrySet())
            System.out.println("Cardinality of the connected component and relative number of N nodes: " + el.getKey().count() + " = " + el.getValue());

        System.out.println("If x=2, the number of vertices in the x-neighbor of "+N.get(1).toString()+" is:"+xNeighbors(graph,N.get(1).toString(),2).count());

        GraphFrame g=filterByNeighbors(graph,N,2,false);
        System.out.println("Number of vertices in the subgraph of N and its x-neighbors: "+g.vertices().count());

        //exportToTsv(g,spark);
        //System.out.println("The graph of N and its x-neighbors is saved in the output folder");

        String url=local ? "bolt://localhost:7687": args[1];
        String user=local ? "neo4j":args[2];
        String password=local ? "Cirociro94":args[3];
        String[] nodesProp=new String[]{"name"};
        String[] edgeProp=new String[]{"alt_id_A", "alt_id_B"};
        


        loadSubgraphToNeo4j(url,user,password,"subgraph1",g);
        System.out.println("Loaded in neo4j the edges describing the subgraph of N and its x-neighbors");
    }

    public static void loadSubgraphToNeo4j(String url, String user, String password,String reltype,GraphFrame graph){
        Driver driver = GraphDatabase.driver(url, AuthTokens.basic(user, password));
        Session s =driver.session();
        String cql;

        String[] prova=new String[]{"p8,p13","p1,p4"};
        for(Row r:graph.edges().toJavaRDD().collect()){
            cql="MATCH (a),(b) WHERE a.name='"+r.getString(0)+"' AND b.name='"+r.getString(1)+"' CREATE (a)-[r:"+reltype+"]->(b)";
            s.run(cql);
        }
        s.close();
    }

    public static void exportToTsv(GraphFrame g,SparkSession spark) {
        spark.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        spark.sparkContext().hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

        g.edges().coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").csv("output");
        File f = new File("output");

        for(String s:f.list()){
            if(s.endsWith("crc")){
                File file=new File("output/"+s);
                file.delete();
            }else{
                File file=new File("output/"+s);
                File file1=new File("output/graph");
                file.renameTo(file1);
            }
        }
    }
    public static Dataset<Row> F1(GraphFrame graph, ArrayList<Object> N, int x) {
        if (x > 0) {
            Dataset<Row> paths = graph.shortestPaths().landmarks(N).run();
            Dataset<Row> explodedPaths = paths
                    .select(paths.col("id"), org.apache.spark.sql.functions.explode(paths.col("distances")))
                    .filter("value<=" + x)
                    .drop("key")
                    .drop("value")
                    .distinct();
            Dataset<Row> edges = graph.edges().join(explodedPaths, graph.edges().col("src").equalTo(explodedPaths.col("id")));
            edges = edges.withColumnRenamed("id", "id1");
            edges = edges.join(explodedPaths, edges.col("dst").equalTo(explodedPaths.col("id")));
            edges = edges.withColumnRenamed("id", "id2");
            graph = GraphFrame.fromEdges(edges);
        }

        Dataset<Row> components = graph.connectedComponents().run();

        Tuple2<Long, Integer> max = components.javaRDD()
                .mapToPair(r -> new Tuple2<>(r.get(1), r.get(0)))
                .mapToPair(new Ncounter(N))
                .reduceByKey(Integer::sum)
                .max(new NCountComparator());

        return components.filter("component=" + max._1).select("id");
    }

    public static HashMap<Dataset<Row>, Integer> F2(GraphFrame graph, ArrayList<Object> N) {

        return F2(graph, N, 0);
    }

    public static HashMap<Dataset<Row>, Integer> F2(GraphFrame graph, ArrayList<Object> N, int x) {

        if (x > 0) {
            Dataset<Row> paths = graph.shortestPaths().landmarks(N).run();
            Dataset<Row> explodedPaths = paths
                    .select(paths.col("id"), org.apache.spark.sql.functions.explode(paths.col("distances")))
                    .filter("value<=" + x)
                    .drop("key")
                    .drop("value")
                    .distinct();
            Dataset<Row> edges = graph.edges().join(explodedPaths, graph.edges().col("src").equalTo(explodedPaths.col("id")));
            edges = edges.withColumnRenamed("id", "id1");
            edges = edges.join(explodedPaths, edges.col("dst").equalTo(explodedPaths.col("id")));
            edges = edges.withColumnRenamed("id", "id2");
            graph = GraphFrame.fromEdges(edges);
        }

        Dataset<Row> components = graph.connectedComponents().run();


        JavaPairRDD<Long, Integer> intersections = components.javaRDD()
                .mapToPair(r -> new Tuple2<>(r.get(1), r.get(0)))
                .mapToPair(new Ncounter(N))
                .reduceByKey(Integer::sum);

        HashMap<Dataset<Row>, Integer> component_count = new HashMap<>();

        for (Tuple2<Long, Integer> t : intersections.collect()) {
            if (t._2 > 0)
                component_count.put(components.filter("component=" + t._1).select("id"), t._2);
        }

        return component_count;
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
    private static class Ncounter implements PairFunction<Tuple2<Object,Object>, Long, Integer> {
        private final ArrayList<Object> N;

        public Ncounter(ArrayList<Object> N) {
            this.N=N;
        }

        @Override
        public Tuple2<Long, Integer> call(Tuple2<Object, Object> t) {
            return new Tuple2<>(Long.parseLong(t._1.toString()), N.contains(t._2)? 1 : 0);
        }
    }

    //F4
    public static GraphFrame filterByNeighbors(GraphFrame graph, ArrayList<Object> N, int x,boolean KEEP){

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
    private static class NCountComparator implements Comparator<Tuple2<Long,Integer>>, Serializable {

        @Override
        public int compare(Tuple2<Long, Integer> t1, Tuple2<Long, Integer> t2) {
            if(t1._2 > t2._2) {
                return 1;
            }
            else if (t1._2 < t2._2) {
                return -1;
            }
            return 0;
        }
    }




}
