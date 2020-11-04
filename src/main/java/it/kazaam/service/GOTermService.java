package it.kazaam.service;

import com.google.common.collect.Sets;
import it.kazaam.repository.GONeo4jRepository;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.neo4j.spark.Neo4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ppiscala.graphUtil;
import ppispark.util.IOfunction;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class GOTermService {
    @Autowired
    private AnnotationService annotationService = null;
    @Autowired
    private GONeo4jRepository goNeo4jRepository = null;
    @Autowired
    private JavaSparkContext jsc;

    private GraphFrame graph;

    public Double goTermSimilarity(Set<Long> terms1, Set<Long> terms2) {
        SparkSession spark = new SparkSession(JavaSparkContext.toSparkContext(jsc));
        graph = IOfunction.GoImport(spark,"51.178.139.69:7687","neo4j","4dm1n1str4t0r");

        int i = 0;
        double average = 0.0;
        for (Long t : terms1) {
            average += (maxSimilarityBetweenTerms(t, terms2) / terms1.size());
            i++;
        }
        return average;
    }

    private Double maxSimilarityBetweenTerms(Long term, Set<Long> terms) {
        return Collections.max(terms.stream().map(t -> goSimilarityJin(term, t)).collect(Collectors.toList()));
    }

    public Double goSimilarityJin(Long id1, Long id2) {
        Double share = shareGrasm(id1, id2);
        return 1 / ((goIC(id1) + goIC(id2) - 2 * share) + 1);
    }

    /**
     * IC of the node identified by the given id. Since we are interested in working with "biological process"
     * aspect, I hard coded its IC.
     *
     * @param id
     * @return
     */
    public Double goIC(Long id) {
        List<Long> successors = goNeo4jRepository.findSuccessors(id);
        double probability = 0d;
        Long maxFreq = maxFrequence("biological process");
        for (Object node : successors) {
            double occurrency = annotationService.countByGOId(Long.parseLong(node.toString().split("\\.")[0])) + 0d;
            probability += occurrency / maxFreq;
        }
        return -Math.log(probability);
    }

    public Double shareGrasm(Long id1, Long id2) {
        Set<Long> commDisjAncestor = getCommDisjAncestors(id1, id2);
        double average = 0.0;
        for (Long ancestor : commDisjAncestor) {
            average += (goIC(ancestor) / commDisjAncestor.size());
        }
        return average;
    }

    private Set<Long> getCommDisjAncestors(Long id1, Long id2) {
        Set<Long> commDisjAncestors = new HashSet<>();
        long[] commAncestors = getCommonAncestors(id1, id2);
        Set<Tuple2<Long, Long>> disjAnc = getDisjAncestors(id1, getAncestors(id1));
        disjAnc.addAll(getDisjAncestors(id2, getAncestors(id2)));
        List<Tuple2<Long, Double>> ic_values = new ArrayList<>();
        for (long id : commAncestors) {
            Double ic = goIC(id);
            ic_values.add(new Tuple2<>(id, ic));
        }
        ic_values.sort((x, y) -> x._2.compareTo(y._2));
        for (int i = 0; i < ic_values.size() - 1; i++) {
            for (int j = i + 1; j < ic_values.size(); j++) {
                if (disjAnc.contains(new Tuple2<>(ic_values.get(i)._1, ic_values.get(j)._1))) {
                    commDisjAncestors.add(ic_values.get(i)._1);
                }
            }
        }
        return commDisjAncestors;
    }

    /**
     * Return the intersection between the ancestors of first GOTerm and second GOTerm
     *
     * @param id1 first GOTerm id
     * @param id2 second GOTerm id
     * @return Common ancestors
     */
    private long[] getCommonAncestors(Long id1, Long id2) {
        //return Sets.intersection(getAncestors(id1), getAncestors(id2));
        return graphUtil.commonAncestors(graph, id1, id2);
    }

    /**
     *  ========== OPERAZIONE COSTOSA DA OTTIMIZZARE ===========
     * Identify the Disjoint Ancestors of the given GOTerm c. It exploits Algorithm 2 of Example 4.2 of the article
     * "Measuring Semantic Similarity between Gene Ontology Terms"
     *
     * @param c         the GOTerm's disjoint ancestors i want to find
     * @param ancestors the ancestors of the GOTerm
     * @return A set of couple (GOTerm_1, GOTerm_2) representing the disjoint ancestors of "c"
     */
    public Set<Tuple2<Long, Long>> getDisjAncestors(Long c, Set<Long> ancestors) {
        Set<Tuple2<Long, Long>> result = new HashSet<>();
        for (Long a1 : ancestors) {
            for (Long a2 : ancestors) {
                if (!a2.equals(a1))
                    // Verifico che a1 non sia negli ancestori di a2 e viceversa
                    if ((!getAncestors(a1).contains(a2)) &&
                            !(getAncestors(a2).contains(a1))) {
                        result.add(new Tuple2<Long, Long>(a1, a2));
                        // Se a1 appartiene agli ancestori di a2, verifico che a2 non sia negli ancestori di a1
                    } else if (!(getAncestors(a2).contains(a1))) {
                        // Verifico che, tra tutti i cammini da a1 a c, ce ne sia almeno uno che non contenga a2
                        List<List<Long>> paths = getPaths(a2, c);
                        boolean found = false;
                        for (int k = 0; k < paths.size() && !found; k++) {
                            // se il cammino i-esimo, tra tutti i cammini da a1 a c, non contiene a2, aggiungo la coppia (a1, a2) ai risultati e mi fermo
                            if (!paths.get(k).contains(a2)) {
                                result.add(new Tuple2<Long, Long>(a1, a2));
                                found = true;
                            }
                        }
                    }
            }
        }
        return result;
    }

    public Set<Long> getAncestors(Long node) {
        Set<Long> ancestors = new HashSet<>();
        ancestors.add(node);
        for (List<Long> path : goNeo4jRepository.findAncestors(node)) {
            ancestors.addAll(path);
        }
        return ancestors;
    }

    public List<List<Long>> getPaths(Long startNode, Long endNode) {
        return goNeo4jRepository.findPathsFromNodeToNode(startNode, endNode);
    }

    public Long maxFrequence(String aspect) {
        switch (aspect) {
            case "molecular function":
                return annotationService.countByGOId(3674L);
            case "cellular component":
                return annotationService.countByGOId(5575L);
            case "biological process":
                return annotationService.countByGOId(8150L);
        }
        return null;
    }
}
