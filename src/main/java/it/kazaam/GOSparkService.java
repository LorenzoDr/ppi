package it.kazaam;

import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;
import ppiscala.sparkServiceUtils;
import ppispark.util.IOfunction;
import scala.Tuple2;

import java.util.Set;

public class GOSparkService {

    private final Graph<Long, Long> graph;

    public GOSparkService(String master, String neo4j_uri, String neo4j_user, String neo4j_pass) {
        SparkContext spark = new SparkContext(master, "GOSparkService");
        graph = IOfunction.GoImport(spark, neo4j_uri.substring(neo4j_uri.lastIndexOf('/')+1), neo4j_user, neo4j_pass);
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
}
