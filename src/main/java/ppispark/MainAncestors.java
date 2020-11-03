package ppispark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import ppiscala.graphUtil;
import ppispark.util.PPInetwork;

public class MainAncestors {
    public static void main(String[] args) {
        SparkSession spark;
        boolean local=true;

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

        PPInetwork ppi = new PPInetwork(spark, args[0]);

        graphUtil.commonAncestors(ppi.getGraph(),"uniprotkb:P51585","uniprotkb:P51591",spark);


    }
}
