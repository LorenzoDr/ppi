package it.kazaam;

import com.google.common.collect.Sets;
import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;
import ppiscala.graphUtil;
import ppispark.util.IOfunction;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collectors;

public class GOTermService {

    private final AnnotationService annotationService;
    private final GONeo4JService goNeo4JService;
    private final SparkContext spark;
    private final Graph<Long, Long> graph;

    public GOTermService(String uri, String user, String pass, String master, AnnotationService annotationService) {
        this.annotationService = annotationService;
        goNeo4JService = new GONeo4JService(uri, user, pass);
        spark = new SparkContext(master, "PPISpark");
        graph = IOfunction.GoImport(spark,uri.substring(uri.lastIndexOf('/')+1), user, pass).cache();
    }

    public Double goTermSimilarity(Set<Long> terms1, Set<Long> terms2) {
        return 0.0;

////        int i = 0;
//        double average = 0.0;
//
//        for (Long t : terms1) {
//            average += (maxSimilarityBetweenTerms(t, terms2) / terms1.size());
////            i++;
//        }
//
//        return average;
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
     * @param id node ID
     * @return IC
     */
    public Double goIC(Long id) {
        List<Long> successors = goNeo4JService.findSuccessors(id);
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
        Set<Long> commAncestors = getCommonAncestors(id1, id2);
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
    private Set<Long> getCommonAncestors(Long id1, Long id2) {
//        return Sets.intersection(getAncestors(id1), getAncestors(id2));
        return JavaConverters.setAsJavaSetConverter(graphUtil.commonAncestors(graph, id1, id2)).asJava();
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
                        result.add(new Tuple2<>(a1, a2));
                        // Se a1 appartiene agli ancestori di a2, verifico che a2 non sia negli ancestori di a1
                    } else if (!(getAncestors(a2).contains(a1))) {
                        // Verifico che, tra tutti i cammini da a1 a c, ce ne sia almeno uno che non contenga a2
                        List<List<Long>> paths = getPaths(a2, c);
                        boolean found = false;
                        for (int k = 0; k < paths.size() && !found; k++) {
                            // se il cammino i-esimo, tra tutti i cammini da a1 a c, non contiene a2, aggiungo la coppia (a1, a2) ai risultati e mi fermo
                            if (!paths.get(k).contains(a2)) {
                                result.add(new Tuple2<>(a1, a2));
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
        for (List<Long> path : goNeo4JService.findAncestors(node)) {
            ancestors.addAll(path);
        }
        return ancestors;
    }

    public List<List<Long>> getPaths(Long startNode, Long endNode) {
        return goNeo4JService.findPathsFromNodeToNode(startNode, endNode);
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