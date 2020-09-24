package ppispark.connectedComponents;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import scala.Tuple2;

public class ConnectedComponents {
	public static void main(String[] args) {

		////////////////////Initialize Spark///////////////////////////
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);

		boolean local = true;

		// String path = local ? "data/human_small.tsv" : args[0];

		SparkConf sparkConf = new SparkConf().setAppName("PPI-project").set("spark.neo4j.url", "bolt://localhost:7687")
				.set("spark.neo4j.user", "neo4j").set("spark.neo4j.password", "Cirociro94");

		SparkSession spark;
		String CheckPath;

		if (local) {
			System.setProperty("hadoop.home.dir", "C:\\Users\\loren\\eclipse\\winutils");
			System.setProperty("spark.sql.legacy.allowUntypedScalaUDF", "true");
			spark = SparkSession.builder().config(sparkConf).master("local[*]")
					// .appName("biograph")
					.getOrCreate();
			CheckPath = "C://Users/loren/eclipse-worskspace/PPI-Project/data/checkpoints";
		} else {
			spark = SparkSession.builder().master("yarn").appName("biograph").config("spark.executor.instances", "8")
					.config("spark.executor.cores", "6").config("spark.executor.memory", "20g")
					.config("spark.debug.maxToStringFields", "50").getOrCreate();
			CheckPath = "hdfs://master.local:8020/user/hduser/data/checkpoint";
		}


		PPInetwork ppi=new PPInetwork(spark,"data/ridotto.tsv");
		ArrayList<Object> landmarks=new ArrayList<Object>();
		landmarks.add("uniprotkb:P51581");
		landmarks.add("uniprotkb:P51584");
		ppi.F7(landmarks,3);





	}



	

}
