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


}