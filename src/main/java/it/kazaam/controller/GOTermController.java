package it.kazaam.controller;


import it.kazaam.service.AnnotationService;
import it.kazaam.service.GOTermService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("")
public class GOTermController {
    @Autowired
    private GOTermService goTermService = null;
    @Autowired
    private AnnotationService annotationService = null;

    /*
     *   CALCOLO DELLA SIMILARITA' TRA PROTEINE
     */
    @PostMapping("/protein/similarity")
    public List<Tuple3<String, String, Double>> proteinSimilarity(@RequestParam List<String> proteins) {
        String protein1 = proteins.get(0);
        String protein2 = proteins.get(1);
        // Identifico i termini associati alle due proteine
        Set<Long> terms1 = annotationService.getDistinctGOTermByProtein(proteins.get(0));
        Set<Long> terms2 = annotationService.getDistinctGOTermByProtein(proteins.get(1));
        // Calcolo la similarità semantica di p1 con p2
        double p1 = goTermService.goTermSimilarity(terms1, terms2);
        double p2 = goTermService.goTermSimilarity(terms2, terms1);
        double similarity = (p1 + p2) / 2;
        // Restituisco i risultati
        List<Tuple3<String, String, Double>> result = new ArrayList<>();
        result.add(new Tuple3<>(protein1, protein2, similarity));
        return result;
    }
}
