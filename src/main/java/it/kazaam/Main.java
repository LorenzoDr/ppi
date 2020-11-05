package it.kazaam;

import scala.Tuple3;

import java.util.Set;

public class Main {

    public static void main(String[] args) {
        String protein1 = args[0];
        String protein2 = args[1];
        String master = "local[*]";

        boolean our_db = false;
        String ip_neo4j = our_db ? "35.195.207.150" : "51.178.139.69";
        String pass = our_db? "ppinetwork" : "4dm1n1str4t0r";

        AnnotationService annotationService = new AnnotationService("mongodb://root:4dm1n1str4t0r@51.178.139.69:9086/?authSource=admin");
        GOTermService goTermService = new GOTermService(String.format("bolt://%s:7687", ip_neo4j), "neo4j", pass, master, annotationService);

        // Identifico i termini associati alle due proteine
        Set<Long> terms1 = annotationService.getDistinctGOTermByProtein(protein1);
        Set<Long> terms2 = annotationService.getDistinctGOTermByProtein(protein2);

        System.out.println(terms1);
        System.out.println(terms2);

        // Calcolo la similarità semantica di p1 con p2
        double p1 = goTermService.goTermSimilarity(terms1, terms2);
        double p2 = goTermService.goTermSimilarity(terms2, terms1);
        double similarity = (p1 + p2) / 2;

        System.out.println(new Tuple3<>(protein1, protein2, similarity));
    }

}