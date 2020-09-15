package ppispark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import ppispark.connectedComponents.Ncounter;
import ppispark.connectedComponents.comparator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        boolean local = false;

        String path = local ? "data/human_small.tsv" : args[0];

        SparkSession spark;

        if (local)
            spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("biograph")
                .getOrCreate();
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

        spark.sparkContext().setCheckpointDir("hdfs://master.local:8020/user/hduser/data/checkpoint");

//        System.out.println("Number of vertices: " + graph.vertices().count());
//        System.out.println("Number of edges: " + graph.edges().count());
//        System.out.println("Number of computed degrees: " + graph.degrees().count());
//        System.out.println("Number of computed triangleCounts: " + graph.triangleCount().run().count());
//
//        System.out.println("Number of BFS results from Q75760 to Q16531: " + graph.bfs().fromExpr("id = 'uniprotkb:Q75760'").toExpr("id = 'uniprotkb:Q16531'").run().count());
//        System.out.println("Number of Connected Components :" + graph.connectedComponents().run().groupBy("component").count().count());
//
//        ArrayList<Object> landmarks = new ArrayList<>();
//        landmarks.add("uniprotkb:B3KV54");
//        landmarks.add("uniprotkb:Q16531");
//
//        graph.shortestPaths().landmarks(landmarks).run().show(5);
//
//        graph.bfs().fromExpr("id = 'uniprotkb:B3KV54'").toExpr("id = 'uniprotkb:Q16531'").run().show(5);
//
//        graph.connectedComponents().run().show(5);

        Row[] rows = (Row[]) graph.vertices().head(100);

        ArrayList<String> N = new ArrayList<>();

        for (Row row : rows)
            N.add(row.getString(0));

        System.out.println("Number of vertices of the graph: " + graph.vertices().count());
    }


}
