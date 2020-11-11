package it.kazaam;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import scala.Tuple3;

import java.util.Set;

public class Main {

    public static void main(String[] args) {
        String protein1 = args[0];
        String protein2 = args[1];
        String master = "yarn";

        boolean our_neo4j = false;

        String neo4j_ip = our_neo4j ? "35.195.207.150" : "51.178.139.69";
        String neo4j_port = "7687";
        String neo4j_user = "neo4j";
        String neo4j_pass = our_neo4j? "ppinetwork" : "4dm1n1str4t0r";

        boolean our_mongo = false;

        String mongo_ip = our_mongo? "35.228.93.232" : "51.178.139.69";
        String mongo_port = our_mongo? "27017" : "9086";
        String mongo_user = "root";
        String mongo_pass = our_mongo? "ppinetwork" : "4dm1n1str4t0r";

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        long start = System.currentTimeMillis();

        SparkContext spark = new SparkContext(master, "GOSparkService");

        spark.conf().
                set("spark.neo4j.url", String.format("bolt://%s:%s", neo4j_ip, neo4j_port)).set("spark.neo4j.user", neo4j_user).set("spark.neo4j.password", neo4j_pass).
                set("spark.mongodb.input.uri", String.format("mongodb://%s:%s@%s:%s/protein-db.annotation?authSource=admin", mongo_user, mongo_pass, mongo_ip, mongo_port));


        GOTermService goTermService = new GOTermService(spark);

        // Calcolo la similarità semantica di p1 con p2
        double similarity = goTermService.goSimilarity(protein1, protein2);

        System.out.printf("T(m) total: %.3f\n", ((System.currentTimeMillis() - start) / 60.0 / 1000));

        System.out.println(new Tuple3<>(protein1, protein2, similarity));
    }

}