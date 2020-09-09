package experiments;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

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
	
	if (local) {
	System.setProperty("hadoop.home.dir", "C:\\Users\\loren\\eclipse\\winutils");
	System.setProperty("spark.sql.legacy.allowUntypedScalaUDF", "true");
	spark = SparkSession.builder().config(sparkConf).master("local[*]")
		// .appName("biograph")
		.getOrCreate();
	} else
	spark = SparkSession.builder().master("yarn").appName("biograph").config("spark.executor.instances", "8")
		.config("spark.executor.cores", "6").config("spark.executor.memory", "20g")
		.config("spark.debug.maxToStringFields", "50").getOrCreate();
	//////////////////////GRAPH FROM TSV///////////////////////////////
	GraphManager g = new GraphManager();
	GraphFrame ppi_network = g.fromTsv(spark, "data/BRCA2.tsv");
	
	//N is list
      List<String> N = new ArrayList<String>(); 
	  N.add("uniprotkb:Q39009");
	  N.add("uniprotkb:O43502");
	  N.add("uniprotkb:Q14565");
	  N.add("intact:EBI-1639774");
	  N.add("uniprotkb:Q8R4X4");
	  String CheckPath="C://Users/loren/eclipse-worskspace/PPI-Project/data/checkpoints";
	 
	
	  //N is a subgraph
	  GraphFrame N1=ppi_network.filterVertices("id IN ('uniprotkb:Q39009','uniprotkb:O43502','uniprotkb:Q14565','intact:EBI-1639774','uniprotkb:Q8R4X4')");

	  boolean list=false;
	  
	  if (list){
		GraphFrame output=g.connectedComponent(ppi_network, 3, spark, CheckPath, N);
		output.vertices().show();
		output.edges().show();
		  
	  }else {
		GraphFrame output=g.connectedComponent(ppi_network, 2, spark, CheckPath, N1);
		output.vertices().show();
		output.edges().show(); 
	  }

	
	}
}
