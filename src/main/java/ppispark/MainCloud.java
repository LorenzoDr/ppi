package ppispark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;
import ppiscala.graphUtil;
import ppispark.util.GraphMiner;
import ppispark.util.IOfunction;
import ppispark.util.PPInetwork;

import java.util.Arrays;

public class MainCloud {
    public static void main(String[] args) {
      /*  SparkSession spark;
        String Isource = args[1];
        String Odest = args[2];

        String[] Iparameters;
        String[] Oparameters;
        int weightIndex = 0;
        PPInetwork ppi;
        boolean local = true;

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);*/
        SparkSession spark;
        boolean local=true;
        if (local) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\loren\\eclipse\\winutils");
            System.setProperty("spark.sql.legacy.allowUntypedScalaUDF", "true");
            spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("biograph")
                    .getOrCreate();
        } else {
            spark = SparkSession.builder()
                    .master("yarn")
                    .appName("biograph")
                    .getOrCreate();
        }

        graphUtil.fromNeo4j(spark,"bolt://localhost:7687","neo4j","Cirociro94","protein");

       /* switch (Isource) {
            case "neo4j":
                Iparameters = args[3].split(",");
                ppi = new PPInetwork(spark, Iparameters[0], Iparameters[1], Iparameters[2], Iparameters[3]);
                weightIndex = 42;
                break;
            case "mongodb":
                Iparameters = args[3].split(",");
                //mongodb://localhost:27017/PPI-network.Edges
                ppi = new PPInetwork(spark, Iparameters[0] + "/" + Iparameters[1] + "." + Iparameters[2], Iparameters[3], Iparameters[4]);
                weightIndex = 42;
                break;
            default:
                ppi = new PPInetwork(spark, args[3]);
                weightIndex = 42;
                break;
        }*/
        //Dataset<Row> edges=ppi.getGraph().edges().withColumn("weight", org.apache.spark.sql.functions.lit(0));
        //GraphFrame graph1=GraphFrame.fromEdges(edges);
        //graphUtil.dijkstra(graph1,"uniprotkb:P51587",42,spark);

       /* String[] functionArgs=Arrays.copyOfRange(args,5,args.length);
        GraphFrame output=GraphMiner.apply(ppi,spark,args[0],functionArgs,weightIndex);


        switch (Odest) {
            case "neo4j":
                IOfunction.exportToTsv(spark,output,args[4]);
                break;
            case "mongodb":
                Oparameters=args[4].split(",");
                //IOfunction.toMongoDB(spark,output,"mongodb://localhost:27017/PPI-network.Edges","");
                IOfunction.toMongoDB(spark,output,Oparameters[0]+"/"+Oparameters[1],Oparameters[2]);
                //IOfunction.toMongoDB(spark,ppi.getGraph(),Oparameters[0]+"/"+Oparameters[1],Oparameters[2]);
                break;
            default:
                IOfunction.exportToTsv(spark,output,args[4]);
                break;
        }*/
    }
}
