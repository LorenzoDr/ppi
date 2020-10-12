package ppispark.functions;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.graphx.Edge;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import ppiscala.graphUtil;

public class test {
	public static void main(String[] args) throws IOException {

		////////////////////Initialize Spark///////////////////////////
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);

		boolean local = true;

		// String path = local ? "data/human_small.tsv" : args[0];

		SparkConf sparkConf = new SparkConf().setAppName("PPI-project");
			//	.set("spark.neo4j.url", "bolt://localhost:7687")
			//	.set("spark.neo4j.user", "neo4j").set("spark.neo4j.password", "Cirociro94");

		SparkSession spark;


		if (local) {
			System.setProperty("hadoop.home.dir", "C:\\Users\\loren\\eclipse\\winutils");
			System.setProperty("spark.sql.legacy.allowUntypedScalaUDF", "true");
			spark = SparkSession.builder().config(sparkConf).master("local[*]")
					// .appName("biograph")
					.getOrCreate();
		} else {
			spark = SparkSession.builder().master("yarn").appName("biograph").config("spark.executor.instances", "8")
					.config("spark.executor.cores", "6").config("spark.executor.memory", "20g")
					.config("spark.debug.maxToStringFields", "50").getOrCreate();
		}


		PPInetwork ppi=new PPInetwork(spark,"data/ridotto.tsv");

		/*String[] nodesProperties=new String[]{"name","type","value"};
		String[] nodesConditions=new String[]{"type<>'C'","value in range(1,20)"};
		String[] edgesProperties=new String[]{"weight","type"};
		String[] edgesConditions=new String[]{"type<>'C'"};
		GraphFrame g=ppi.filteredEdgesFromNeo4j("bolt://localhost:7687","neo4j","Cirociro94",nodesProperties,edgesProperties,edgesConditions);
		GraphFrame g=ppi.importGraphFromNeo4j("bolt://localhost:7687","neo4j","Cirociro94",nodesProperties,edgesProperties,nodesConditions,edgesConditions,false);
*/
		ArrayList<Object> nodes=new ArrayList<Object>();
		nodes.add("uniprotkb:P51583");
		nodes.add("uniprotkb:P51027");
		nodes.add("uniprotkb:P51028");
		nodes.add("uniprotkb:P51072");
		nodes.add("uniprotkb:P51075");
		nodes.add("uniprotkb:P51076");
		nodes.add("uniprotkb:P51071");

		//GraphFrame g=ppi.importEdgesFromMongoDB("mongodb://localhost:27017/PPI-network.Edges","src","dst");
		//GraphFrame g=ppi.importGraphFromMongoDB("mongodb://localhost:27017/PPI-network.Edges","mongodb://localhost:27017/PPI-network.Nodes","name","src","dst");
		//g.edges().show();
		//g.vertices().show();
		ppi.toMongoDB();
		//ppi.exportToTsv("graph");
		//ppi.loadSubgraphToNeo4j("bolt://localhost:7687","ciro","Cirociro94","rel");


	}



	

}
