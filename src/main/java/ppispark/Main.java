package ppispark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

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

        spark.sparkContext().setCheckpointDir(local ? "checkpoint" : "hdfs://master.local:8020/user/hduser/data/checkpoint");

        Row[] rows = (Row[]) graph.vertices().head(100);

        ArrayList<Object> N = new ArrayList<>();

        for (Row row : rows)
            N.add(row.getString(0));

        System.out.println("Number of vertices of the connected component: " + F1(graph, N, 4).vertices().count());
        System.out.println("Connected component and relative number of nodes of N: " + F2(graph, N, 0));
    }

    public static GraphFrame F1(GraphFrame graph, ArrayList<Object> N, int x) {
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

        return extractComponent(graph, components, max._1);
    }

    public static HashMap<Long, Integer> F2(GraphFrame graph, ArrayList<Object> N, int x) {

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

        HashMap<Long, Integer> component_count = new HashMap<>();

        for (Tuple2<Long, Integer> t : intersections.collect()) {
            if (t._2 > 0)
                component_count.put(t._1, t._2);
        }

        return component_count;
    }

    private static GraphFrame extractComponent(GraphFrame graph, Dataset<Row> components, long componentID) {
        Dataset<Row> maxComponent = components.filter("component=" + componentID);
        Dataset<Row> filteredEdges = graph.edges().join(maxComponent, graph.edges().col("src").equalTo(maxComponent.col("id")));

        return GraphFrame.fromEdges(filteredEdges);
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
