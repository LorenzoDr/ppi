package ppispark.util;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.spark.Neo4j;
import ppiscala.graphUtil;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.File;
import java.util.HashMap;

public class IOfunction {
    //TSV FILE
    public static GraphFrame importFromTsv(SparkSession spark,String path) {
        String[] cols_names = new String[] { "src", "dst", "alt_id_A", "alt_id_B", "alias_A", "alias_B", "det_method",
                "first_auth", "id_pub", "ncbi_id_A", "ncbi_id_B", "int_types", "source_db", "int_id", "conf_score",
                "comp_exp", "bio_role_A", "bio_role_B", "exp_role_A", "exp_role_B", "type_A", "type_B", "xref_A",
                "xref_B", "xref_int", "annot_A", "annot_B", "annot_int", "ncbi_id_organism", "param_int", "create_data",
                "up_date", "chk_A", "chk_B", "chk_int", "negative", "feat_A", "feat_B", "stoich_A", "stoich_B",
                "part_meth_A", "part_meth_B" };

        Dataset<Row> edges = spark.read().
                option("header", "True").
                option("sep", "\t").format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"). //delimiter?
                load(path);


        edges = edges.toDF(cols_names);//.withColumn("new_col",functions.lit(1));
        GraphFrame graph = GraphFrame.fromEdges(edges);
        return graph;
    }

    public static void exportToTsv(SparkSession spark,GraphFrame g,String outputName) {
        spark.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        spark.sparkContext().hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

        g.edges().coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "true").option("delimiter", "\t").save(outputName);
        File f = new File("output");

      /*  for(String s:f.list()){
            if(s.endsWith("crc")){
                File file=new File("output/"+s);
                file.delete();
            }else{
                File file=new File("output/"+s);
                File file1=new File(filename+"_ppi");
                file.renameTo(file1);
                f.delete();
            }
        }*/
    }

    //MongoDB
    public  static GraphFrame importEdgesFromMongoDB(SparkSession spark,String uri, String src, String dst) {
        SparkContext sc=spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        jsc.sc().conf()
                .set("spark.mongodb.input.uri", "mongodb://"+uri)
                .set("spark.mongodb.output.uri", "mongodb://"+uri);

        Dataset<Row> edges = MongoSpark.load(jsc).toDF();

        edges=edges.drop("_id");
        edges=edges.withColumnRenamed(src,"src");
        edges=edges.withColumnRenamed(dst,"dst");

        GraphFrame g=GraphFrame.fromEdges(edges);
        return g;
    }
    public GraphFrame importGraphFromMongoDB(SparkSession spark,String edgesUri,String nodesUri,String id,String src,String dst) {
        SparkContext sc=spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        jsc.sc().conf()
                .set("spark.mongodb.input.uri", edgesUri)
                .set("spark.mongodb.output.uri", edgesUri);
        Dataset<Row> edges = MongoSpark.load(jsc).toDF();
        edges=edges.drop("_id");
        edges=edges.withColumnRenamed(src,"src");
        edges=edges.withColumnRenamed(dst,"dst");

        jsc.sc().conf()
                .set("spark.mongodb.input.uri", nodesUri)
                .set("spark.mongodb.output.uri", nodesUri);
        Dataset<Row> nodes= MongoSpark.load(jsc).toDF();
        nodes=nodes.drop("_id");
        nodes=nodes.withColumnRenamed(id,"id");

        return GraphFrame.apply(nodes,edges);
    }
    public static void toMongoDB(SparkSession spark,GraphFrame graph,String uri,String collection){
        SparkContext sc=spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        jsc.sc().conf()
                .set("spark.mongodb.input.uri", "mongodb://"+uri+".")
                .set("spark.mongodb.output.uri","mongodb://"+uri+".");
        MongoSpark.write(graph.edges()).option("collection", collection).mode("overwrite").save();
    }

    public void toMongoDB(SparkSession spark,GraphFrame graph,String uri,String edgesCollection,String nodesCollection){
        SparkContext sc=spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        jsc.sc().conf()
                .set("spark.mongodb.input.uri", uri)
                .set("spark.mongodb.output.uri",uri);
        MongoSpark.write(graph.edges()).option("collection", edgesCollection).mode("overwrite").save();
        MongoSpark.write(graph.edges()).option("collection", nodesCollection).mode("overwrite").save();

    }
    //NEO4J
    public static GraphFrame fromNeo4j(SparkSession spark,String url, String user, String password, String id) {
        spark.sparkContext().conf()//.set("spark.neo4j.encryption.status","false")
                .set("spark.neo4j.url", "bolt://"+url)
                .set("spark.neo4j.user", user)
                .set("spark.neo4j.password", password);

        StructType schemaEdges=new StructType().add("src","String").add("dst","String");

        Neo4j conn = new Neo4j(spark.sparkContext());

        RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) RETURN toString(a."+id+"),toString(b."+id+")",JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();

        Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);

        GraphFrame output=GraphFrame.fromEdges(edges);

        return output;
    }

    public static GraphFrame importGraphFromNeo4j(SparkSession spark,String url, String user, String password, String[] nodesProperties, String[] edgesProperties) {
        spark.sparkContext().conf()//.set("spark.neo4j.encryption.status","false")
                .set("spark.neo4j.url", url)
                .set("spark.neo4j.user", user)
                .set("spark.neo4j.password", password);

        StructType schemaVertices=new StructType().add("id","String");
        StructType schemaEdges=new StructType().add("src","String").add("dst","String");

        String nodesString="";
        String edgesString="";
        for(int i=1;i<nodesProperties.length;i++){
            nodesString+=",toString(a."+nodesProperties[i]+")";
            schemaVertices=schemaVertices.add(nodesProperties[i],"String");
        }
        for(int i=0;i<edgesProperties.length;i++){
            edgesString+=",toString(r."+edgesProperties[i]+")";
            schemaEdges=schemaEdges.add(edgesProperties[i],"String");
        }
        Neo4j conn = new Neo4j(spark.sparkContext());


        RDD<Row> rddVertices=conn.cypher("MATCH (a) RETURN toString(a."+nodesProperties[0]+")"+nodesString, JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
        RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) RETURN toString(a."+nodesProperties[0]+"),toString(b."+nodesProperties[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



        Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
        Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);

        GraphFrame output=GraphFrame.apply(vertices,edges);


        return output;
    }
    //FILTER NODES WHILE IMPORTING FROM NEO4J
    public GraphFrame filteredNodesFromNeo4j(SparkSession spark,String url, String user, String password, String[] nodesProperties, String[] edgesProperties,String[] conditions) {

        spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
                .set("spark.neo4j.url", url)
                .set("spark.neo4j.user", user)
                .set("spark.neo4j.password", password);

        StructType schemaVertices=new StructType().add("id","String");
        StructType schemaEdges=new StructType().add("src","String").add("dst","String");

        String nodesString="";
        String edgesString="";
        String filter1="";
        String filter2="";

        for(int i=1;i<nodesProperties.length;i++){
            nodesString+=",toString(a."+nodesProperties[i]+")";
            schemaVertices=schemaVertices.add(nodesProperties[i],"String");
        }
        for(int i=1;i<conditions.length;i++){
            filter1+="AND a."+conditions[i];
            filter2+="AND a."+conditions[i];
            filter2+="AND b."+conditions[i];
        }
        for(int i=0;i<edgesProperties.length;i++){
            edgesString+=",toString(r."+edgesProperties[i]+")";
            schemaEdges=schemaEdges.add(edgesProperties[i],"String");
        }
        Neo4j conn = new Neo4j(spark.sparkContext());


        RDD<Row> rddVertices=conn.cypher("MATCH (a) WHERE a."+conditions[0]+" "+filter1+" RETURN toString(a."+nodesProperties[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
        RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE a."+conditions[0]+" AND b."+conditions[0]+" "+filter2+" RETURN toString(a."+nodesProperties[0]+"),toString(b."+nodesProperties[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



        Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
        Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);


        GraphFrame output=GraphFrame.apply(vertices,edges);

        return output;
    }

    //FILTER EDGES WHILE IMPORTING
    public GraphFrame filteredEdgesFromNeo4j(SparkSession spark,String url, String user, String password, String[] nodesProperties, String[] edgesProperties,String[] conditions,boolean dropVertices) {

        spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
                .set("spark.neo4j.url", url)
                .set("spark.neo4j.user", user)
                .set("spark.neo4j.password", password);

        StructType schemaVertices=new StructType().add("id","String");
        StructType schemaEdges=new StructType().add("src","String").add("dst","String");

        String nodesString="";
        String edgesString="";
        String filter="";


        for(int i=1;i<nodesProperties.length;i++){
            nodesString+=",toString(a."+nodesProperties[i]+")";
            schemaVertices=schemaVertices.add(nodesProperties[i],"String");
        }
        for(int i=1;i<conditions.length;i++){
            filter+="AND r."+conditions[i];
        }
        for(int i=0;i<edgesProperties.length;i++){
            edgesString+=",toString(r."+edgesProperties[i]+")";
            schemaEdges=schemaEdges.add(edgesProperties[i],"String");
        }
        Neo4j conn = new Neo4j(spark.sparkContext());


        RDD<Row> rddVertices=conn.cypher("MATCH (a) RETURN toString(a."+nodesProperties[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
        RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE r."+conditions[0]+filter+" RETURN toString(a."+nodesProperties[0]+"),toString(b."+nodesProperties[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



        Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
        Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);


        GraphFrame output=GraphFrame.apply(vertices,edges);

        if(dropVertices){
            return output.dropIsolatedVertices();
        }else{
            return output;
        }

    }

    public GraphFrame filteredEdgesFromNeo4j(SparkSession spark,String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] conditions){
        return filteredEdgesFromNeo4j(spark,url,user,password,nodesLabels,edgesProperties,conditions,true);
    }

    //FILTER EDGES AND NODES WHILE IMPORTING
    public GraphFrame importGraphFromNeo4j(SparkSession spark,String url, String user, String password, String[] nodesProperties, String[] edgesProperties,String[] nodesConditions,String[] edgesConditions,boolean dropVertices) {

        spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
                .set("spark.neo4j.url", url)//"bolt://localhost:7687"
                .set("spark.neo4j.user", user)//"neo4j"
                .set("spark.neo4j.password",password );//"Cirociro94"

        StructType schemaVertices=new StructType().add("id","String");
        StructType schemaEdges=new StructType().add("src","String").add("dst","String");

        String nodesString="";
        String edgesString="";
        String filter1="";
        String filter2="";

        for(int i=1;i<nodesProperties.length;i++){
            nodesString+=",toString(a."+nodesProperties[i]+")";
            schemaVertices=schemaVertices.add(nodesProperties[i],"String");
        }
        for(int i=1;i<nodesConditions.length;i++){
            filter1+="AND a."+nodesConditions[i];
            filter2+="AND a."+nodesConditions[i];
            filter2+="AND b."+nodesConditions[i];
        }
        for(int i=0;i<edgesProperties.length;i++){
            edgesString+=",toString(r."+edgesProperties[i]+")";
            schemaEdges=schemaEdges.add(edgesProperties[i],"String");
        }
        for(int i=0;i<edgesConditions.length;i++){
            filter2+="AND r."+edgesConditions[i];
        }

        Neo4j conn = new Neo4j(spark.sparkContext());


        RDD<Row> rddVertices=conn.cypher("MATCH (a) WHERE a."+nodesConditions[0]+" "+filter1+" RETURN toString(a."+nodesProperties[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
        RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE a."+nodesConditions[0]+" AND b."+nodesConditions[0]+" "+filter2+" RETURN toString(a."+nodesProperties[0]+"),toString(b."+nodesProperties[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();


        Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
        Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);

        GraphFrame output=GraphFrame.apply(vertices,edges);

        if(dropVertices){
            return output.dropIsolatedVertices();
        }
        else{
            return output;
        }

    }

    public GraphFrame importGraphFromNeo4j(SparkSession spark,String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] nodesCondition,String[] edgesCondition){
        return importGraphFromNeo4j(spark,url,user,password,nodesLabels,edgesProperties,nodesCondition,edgesCondition,true);
    }

   /* public static void toNeo4j(GraphFrame g,SparkSession spark,String url, String user, String password){
        spark.sparkContext().conf()
                .set("spark.neo4j.url", url)
                .set("spark.neo4j.user", user)
                .set("spark.neo4j.password", password);
        graphUtil.toNeo4J(g,spark);
    }*/

    public static void updateSubgraphLabels(GraphFrame graph,String url, String user, String password,String reltype){
        Driver driver = GraphDatabase.driver(url, AuthTokens.basic(user, password));
        Session s =driver.session();
        String cql;

        for(Row r:graph.edges().toJavaRDD().collect()){
            cql="MATCH (a:protein)-[r:RELTYPE]->(b:protein) WHERE a.name='"+r.getString(0)+"' AND b.name='"+r.getString(1)+"' SET r.F4=true";
        }
        s.close();
    }

}