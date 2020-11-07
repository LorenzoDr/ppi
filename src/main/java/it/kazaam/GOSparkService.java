package it.kazaam;

import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;
import org.neo4j.spark.Neo4j;
import ppiscala.sparkServiceUtils;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Set;

public class GOSparkService {

    private final static ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);

    private final Graph<Long, Long> graph;

    public GOSparkService(SparkContext spark) {
        graph = loadGraph(spark);
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

    private Graph<Long, Long> loadGraph(SparkContext spark) {
        return Neo4j.apply(spark).partitions(100).
                pattern(new Tuple2<>("GOTerm", "GOid"), new Tuple2<>("IS_A", "id"), new Tuple2<>("GOTerm", "GOid")).
                loadGraph(longTag, longTag);
    }
}
