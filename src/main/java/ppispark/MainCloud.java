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
        SparkSession spark;
        String Isource=args[0];
        String Osource=args[1];

        //String IOsource="mongo";
        int startingIndex=0;
        int weightIndex=0;
        PPInetwork ppi;
        boolean local=false;

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
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

        //ppi = new PPInetwork(spark, args[1]);

        switch (Isource) {
            case "neo":
                //ppi=new PPInetwork(spark,"bolt://localhost:7687","neo4j","Cirociro94","");
                ppi = new PPInetwork(spark,args[2],args[3],args[4],"");
                startingIndex=5;
                weightIndex=2;
                break;
            case "mongo":
                ppi = new PPInetwork(spark,"mongodb://localhost:27017/PPI-network.Edges","src","dst");
                ppi = new PPInetwork(spark,args[2],args[3],args[4]);
                startingIndex=5;
                weightIndex=4;
                break;
            default:
                //ppi = new PPInetwork(spark, "data/unweightedGraph.tsv");
                ppi = new PPInetwork(spark, args[2]);
                weightIndex=42;
                startingIndex=3;
                break;
        }
        //ppi.vertices().show();
       // ppi.edges().show();

        //String[] functionArgs=new String[]{}; //Arrays.copyOfRange(args,startingIndex,args.length-1);
        GraphFrame output=GraphMiner.apply(ppi,spark,1,args[3],weightIndex);
        //GraphFrame output=GraphMiner.apply(ppi,spark,1,"uniprotkb:P51502",weightIndex);
        //output.edges().show();
        //output.vertices().show();
        //System.out.println("V:"+output.vertices().count());
        //System.out.println("E:"+output.edges().count());

        switch (Osource) {
            case "neo":
                //IOfunction.updateSubgraphLabels(output,"bolt://localhost:7687","neo4j","Cirociro94","");
                IOfunction.toNeo4j(output,spark,args[4],args[5],args[6]);
                break;
            case "mongo":
                //IOfunction.toMongoDB(spark,output,"mongodb://localhost:27017/PPI-network.Edges","");
                IOfunction.toMongoDB(spark,output,args[4],args[5]);
                break;
            default:
                IOfunction.exportToTsv(spark,output,"F4");
                break;
        }
    }
}
