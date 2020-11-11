package it.kazaam;

import org.apache.spark.SparkContext;

import scala.Tuple2;

import java.util.*;

public class GOTermService {
    public final GOSparkService goSparkService;
    public final AnnotationService annotationService;
    private final Map<Long, Long> frequencies;
    private final HashMap<String, Long> maxFrequence;

    public GOTermService(SparkContext spark) {
        annotationService = new AnnotationService(spark);
        goSparkService = new GOSparkService(spark);

        this.frequencies = annotationService.annotations.mapToPair(doc -> new Tuple2<>(doc.getLong("goID"), 1L)).countByKey();

        maxFrequence = new HashMap<>();
        maxFrequence.put("molecular function", annotationService.countByGOId(3674L));
        maxFrequence.put("cellular component", annotationService.countByGOId(5575L));
        maxFrequence.put("biological process", annotationService.countByGOId(8150L));
    }

    public Double goSimilarity(String protein1, String protein2) {
        Set<Long> terms1 = annotationService.getDistinctGOTermByProtein(protein1);
        Set<Long> terms2 = annotationService.getDistinctGOTermByProtein(protein2);

        System.out.println("Terms of " + protein1 + ": " + terms1);
        System.out.println("Terms of " + protein2 + ": " + terms2);

        Map<Long, Double> id_ic_map = computeIC(terms1, terms2);

        System.out.println("IC computed!");

        Map<Long, Map<Long, Double>> similarity = new HashMap<>(terms1.size() + terms2.size());

        for (long t1 : terms1)
            similarity.put(t1, new HashMap<>(terms2.size()));

        for (long t2 : terms2)
            similarity.put(t2, new HashMap<>(terms1.size()));

        for (long t1 : terms1)
            for (long t2 : terms2) {
                long start = System.currentTimeMillis();
                double sim_jin = goSimilarityJin(t1, t2, id_ic_map);
                System.out.println("T(m) similarityJin: " + ((System.currentTimeMillis() - start) / 1000.0 / 60));

                similarity.get(t1).put(t2, sim_jin);
                similarity.get(t2).put(t1, sim_jin);
            }

        return (goTermSimilarity(terms1, terms2, similarity) + goTermSimilarity(terms2, terms1, similarity)) / 2;
    }

    public Double goTermSimilarity(Set<Long> terms1, Set<Long> terms2, Map<Long, Map<Long, Double>> similarity) {
        double avg_sim = 0.0;

        for (Long t1 : terms1) {
            double max_sim = Double.MIN_VALUE;

            for (Long t2 : terms2) {
                double sim = similarity.get(t1).get(t2);

                if (sim > max_sim)
                    max_sim = sim;
            }

            avg_sim += max_sim;
        }

        avg_sim /= terms1.size();

        return avg_sim;
    }

    public Double goTermSimilarity(Set<Long> terms1, Set<Long> terms2) {
        double average = 0.0;

        Map<Long, Double> id_ic_map = computeIC(terms1, terms2);

        System.out.println("IC computed!");

        for (Long t : terms1) {
            average += (maxSimilarityBetweenTerms(t, terms2, id_ic_map) / terms1.size());
        }

        return average;
    }

    private Double maxSimilarityBetweenTerms(Long term, Set<Long> terms, Map<Long, Double> id_ic_map) {
        double max_similarity = Double.MIN_VALUE;

        for (Long t: terms){
            double sim = goSimilarityJin(term, t, id_ic_map);

            if (max_similarity < sim)
                max_similarity = sim;
        }

        return max_similarity;
    }

    public Double goSimilarityJin(Long id1, Long id2, Map<Long, Double> id_ic_map) {
        Double share = shareGrasm(id1, id2, id_ic_map);
        return 1 / ((id_ic_map.get(id1) + id_ic_map.get(id2) - 2 * share) + 1);
    }

    private Map<Long, Double> computeIC(Set<Long> terms1, Set<Long> terms2) {
        Set<Long> ids = new HashSet<>(terms1);
        ids.addAll(terms2);

        for (long term : terms1)
            ids.addAll(goSparkService.getAncestors(term));

        for (long term : terms2)
            ids.addAll(goSparkService.getAncestors(term));

        HashMap<Long, Double> id_ic_map = new HashMap<>();

        for (long term : ids)
            id_ic_map.put(term, goIC(term));

        return id_ic_map;
    }

    /**
     * IC of the node identified by the given id. Since we are interested in working with "biological process"
     * aspect, I hard coded its IC.
     *
     * @param id node ID
     * @return IC
     */
    public Double goIC(Long id) {
//        List<Long> successors = goNeo4JService.findSuccessors(id);
        Set<Long> successors = goSparkService.findSuccessors(id);
        double probability = 0d;
        Long maxFreq = maxFrequence("biological process");
        for (Long node : successors) {
            double occurrency = frequencies.getOrDefault(node, 0L).doubleValue();
            probability += occurrency / maxFreq;
        }

        return -Math.log(probability);
    }

    public Double shareGrasm(Long id1, Long id2, Map<Long, Double> id_ic_map) {
        Set<Long> commDisjAncestor = getCommDisjAncestors(id1, id2, id_ic_map);
        double average = 0.0;
        for (Long ancestor : commDisjAncestor) {
            average += (id_ic_map.get(ancestor) / commDisjAncestor.size());
        }
        return average;
    }

    private Set<Long> getCommDisjAncestors(Long id1, Long id2, Map<Long, Double> id_ic_map) {
        Set<Long> commDisjAncestors = new HashSet<>();

        Set<Long> commAncestors = getCommonAncestors(id1, id2);
//        Set<Tuple2<Long, Long>> disjAnc = getDisjAncestors(id1, getAncestors(id1));
//        disjAnc.addAll(getDisjAncestors(id2, getAncestors(id2)));
        Set<Tuple2<Long, Long>> disjAnc = getDisjAncestors(id1, getAncestors(id1), id2, getAncestors(id2));

        List<Tuple2<Long, Double>> ic_values = new ArrayList<>();
        for (long id : commAncestors) {
            Double ic = id_ic_map.get(id);
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
        return goSparkService.getCommonAncestors(id1, id2);
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
//        Set<Tuple2<Long, Long>> result = new HashSet<>();
//        for (Long a1 : ancestors) {
//            for (Long a2 : ancestors) {
//                if (!a2.equals(a1))
//                    // Verifico che a1 non sia negli ancestori di a2 e viceversa
//                    if ((!getAncestors(a1).contains(a2)) &&
//                            !(getAncestors(a2).contains(a1))) {
//                        result.add(new Tuple2<>(a1, a2));
//                        // Se a1 appartiene agli ancestori di a2, verifico che a2 non sia negli ancestori di a1
//                    } else if (!(getAncestors(a2).contains(a1))) {
//                        // Verifico che, tra tutti i cammini da a1 a c, ce ne sia almeno uno che non contenga a2
//                        List<List<Long>> paths = getPaths(a2, c);
//                        boolean found = false;
//                        for (int k = 0; k < paths.size() && !found; k++) {
//                            // se il cammino i-esimo, tra tutti i cammini da a1 a c, non contiene a2, aggiungo la coppia (a1, a2) ai risultati e mi fermo
//                            if (!paths.get(k).contains(a2)) {
//                                result.add(new Tuple2<>(a1, a2));
//                                found = true;
//                            }
//                        }
//                    }
//            }
//        }
//        return result;

        return goSparkService.getDisjAncestors(c, ancestors);
    }

    public Set<Tuple2<Long, Long>> getDisjAncestors(Long c1, Set<Long> ancestors1, Long c2, Set<Long> ancestors2) {
        return goSparkService.getDisjAncestors(c1, ancestors1, c2, ancestors2);
    }

    public Set<Long> getAncestors(Long node) {
//        Set<Long> ancestors = new HashSet<>();
//        ancestors.add(node);
//        for (List<Long> path : goNeo4JService.findAncestors(node)) {
//            ancestors.addAll(path);
//        }
//        return ancestors;

        return goSparkService.getAncestors(node);
    }

//    public List<List<Long>> getPaths(Long startNode, Long endNode) {
//        return goNeo4JService.findPathsFromNodeToNode(startNode, endNode);
//    }

    public Long maxFrequence(String aspect) {
        return maxFrequence.get(aspect);
    }

//    public void close() {
//        annotationService.close();
//        goNeo4JService.close();
//    }
}
