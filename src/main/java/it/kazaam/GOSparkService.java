package it.kazaam;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.neo4j.spark.Neo4j;
import org.neo4j.spark.Neo4jGraph;
import ppiscala.sparkServiceUtils;
import ppispark.util.IOfunction;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Set;

public class GOSparkService {

    private final static ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
    private final Graph<Long, Long> graph;

    public GOSparkService(String master, String neo4j_uri, String neo4j_user, String neo4j_pass) {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkContext spark = new SparkContext(master, "GOSparkService");
        graph = loadFromNeo4j(spark, neo4j_uri, neo4j_user, neo4j_pass);
    }

    public Set<Long> getAncestors(Long id) {
        return sparkServiceUtils.ancestors(graph, id);
    }

    public Set<Tuple2<Long, Long>> getDisjAncestors(Long id, Set<Long> ancestors) {
        return sparkServiceUtils.disjointAncestors(graph, id, ancestors);
    }

    public Set<Long> getCommonAncestors(Long id1, Long id2) {
        return sparkServiceUtils.commonAncestors(graph, id1, id2);
    }

    public Set<Long> findSuccessors(Long id) {
        return sparkServiceUtils.successors(graph, id);
    }

    private static Graph<Long, Long> loadFromNeo4j(SparkContext spark, String uri, String user, String password) {
        spark.conf().set("spark.neo4j.url", "bolt://"+uri.substring(uri.lastIndexOf('/')+1)).set("spark.neo4j.user", user).set("spark.neo4j.password", password);

        // todo: think about more useful attributes
        return Neo4j.apply(spark).partitions(100).pattern(new Tuple2<>("GOTerm", "GOid"), new Tuple2<>("IS_A", "id"), new Tuple2<>("GOTerm", "GOid")).loadGraph(longTag, longTag);
    }
}
