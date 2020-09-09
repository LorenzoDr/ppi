package experiments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.neo4j.spark.*;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

public class Main {

	public static void main(String[] args) {

		//////////////////// Initialize Spark///////////////////////////
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
		
		///////////////// GRAPH FROM ARRAYLISTS/////////////////////////////
		
		  List<User> vertices = new ArrayList<>();
		  vertices.add(new User("a", "Alice",
		  34)); vertices.add(new User("b", "Bob", 36)); vertices.add(new User("c",
		  "Charlie", 30)); vertices.add(new User("d", "David", 29)); vertices.add(new
		  User("e", "Esther", 32)); vertices.add(new User("f", "Fanny", 36));
		  vertices.add(new User("g", "Gabby", 60));
		  
		  List<Relationship> edges = new ArrayList<>(); 
		  edges.add(new
		  Relationship("Friend", "a", "b")); edges.add(new Relationship("Follow", "b",
		  "c")); edges.add(new Relationship("Follow", "c", "b")); edges.add(new
		  Relationship("Follow", "f", "c")); edges.add(new Relationship("Follow", "e",
		  "f")); edges.add(new Relationship("Friend", "e", "d")); edges.add(new
		  Relationship("Friend", "d", "a")); edges.add(new Relationship("Friend", "a",
		  "e"));
		  
		  GraphFrame social_network = g.fromLists(spark, vertices, edges);
		////////////////// EXPLORATIVE ANALYSIS/////////////////////////////
		 long nVertices=ppi_network.vertices().count();
		 long nEdges=ppi_network.edges().count();		 		  
		 long potentialConnections=(nVertices*(nVertices-1))/2; long
		 density=2*nEdges/potentialConnections; 
		 System.out.println("Number of edges: "+nEdges); 
		 System.out.println("Number of vertices: "+nVertices);
		 System.out.println("Density: "+density); 
		 
		  ppi_network.triangleCount().run().show();
		  ppi_network.degrees().show();
		  ppi_network.inDegrees().show(); 
		  ppi_network.outDegrees().show();
		  //////////////////////////////SUBGRAPH/////////////////////////////
		  
		 /* In GraphFrame there are two ways to obtain a subgraph from a given one: 
		  * 1.Motif Finding Operation, that searches for structural patterns
		  * 2.Filter Operation, that operates on edges or vertices attributes.*/
		  
		  //Motif Finding: the output is a Spark Dataset
		  ppi_network.find("(a)-[]->(b);(b)-[]->(a)").show(); // Search for pairs of vertices with edges in both directions between them.
		  ppi_network.find("(a)-[]->(b);!(b)-[]->(a)").show(); // Search for pairs of vertices with edges in a single direction. 
		  ppi_network.find("(a)-[e]->(b); (b)-[e2]->(c)").show(); // Search patterns where a vertex is a destination of the edge e and the source of edges e2 
		  														  // This query consider also patterns where a and c are the same node
		  //Filter: the output is a GraphFrame
		  ppi_network.filterEdges("alt_id_B=='intact:EBI-16080048'").edges().show(); //Filter on edges
		  ppi_network.filterEdges("alt_id_B=='intact:EBI-16080048'").dropIsolatedVertices().vertices().show(); //consider nodes connected by edges that satisfy the condition
		  ppi_network.filterVertices("id=='uniprotkb:Q75760'").vertices().show(); //filter on vertices 
		
		  //Motif_Finding + Filter 
		  ppi_network.find("(a)-[e]->(b); (b)-[e2]->(c)").filter("a.id != c.id").show(); //exclude patterns where a and c are the same node
		  
		  //////////////////////////CONNECTIVITY/////////////////////////////
		  ArrayList<Object> landmarks= new ArrayList();
		  landmarks.add("f");// "uniprotkb:Q75760"); landmarks.add("b");
		  landmarks.add("b");
		  Dataset<Row> paths=social_network.shortestPaths().landmarks(landmarks).run();	 
		  Dataset<Row> explodedPaths=paths.select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")));
		  
		  explodedPaths
		  .where("key='b' AND value<2")
		  .show();
		  ////////////////////EVALUATING CONNECTED COMPONENTS////////////////////////////
	      spark.sparkContext().setCheckpointDir("C://Users/loren/eclipse-worskspace/PPI-Project/data/checkpoint");
		  social_network.connectedComponents().run().show();
		  
		  
		  /////////////////////CENTRALITY MEASURE////////////////////////////
		  //Closeness
		  Dataset<Row> d=explodedPaths
		  .groupBy("key")
		  .sum("value");

		  d.withColumn("sum(value)",org.apache.spark.sql.functions.pow(d.col("sum(value)"), -1)).show();
		  
		  
		  
		  
		  
	}

}
