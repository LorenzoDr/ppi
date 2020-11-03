package ppispark.util;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.graphframes.GraphFrame;

import ppiscala.graphUtil;
import scala.Tuple2;

public class PPInetwork {

	private SparkSession spark;
	private GraphFrame graph;


	//PPI FROM TSV
	public PPInetwork(SparkSession spark, String inputPath){
		this.spark=spark;
		graph= IOfunction.importFromTsv(spark,inputPath);
		spark.sparkContext().setCheckpointDir("PPI-Check");
	}
	public PPInetwork(SparkSession spark, String inputPath, String CheckPath){
		this.spark=spark;
		graph= IOfunction.importFromTsv(spark,inputPath);
		spark.sparkContext().setCheckpointDir(CheckPath);
	}
	//PPI FROM MONGODB
	public PPInetwork(SparkSession spark,String uri,String src,String dst){
		this.spark=spark;
		graph= IOfunction.importEdgesFromMongoDB(spark,uri,src,dst);
		spark.sparkContext().setCheckpointDir("PPI-Check");

	}
	//PPI FROM NEO4J
	public PPInetwork(SparkSession spark,String url,String user,String password,String id){
		this.spark=spark;
		graph= IOfunction.fromNeo4j(spark,url,user,password,id);
		spark.sparkContext().setCheckpointDir("PPI-Check");

	}

	//ACCESS TO PPI
	public GraphFrame getGraph(){
		return graph;
	}
	public Dataset<Row> vertices(){
		return graph.vertices();
	}
	public long verticesCounter(){
		return graph.vertices().count();
	}
	public Dataset<Row> edges(){
		return graph.edges();
	}
	public void filterOnEdges(String condition){
		graph.filterEdges(condition);
	}
	public long edgesCounter(){
		return graph.edges().count();
	}
	// EXPLORATORY STATISTICS
	public double density(){
		double nVertices=graph.vertices().count();
		double nEdges=graph.edges().count();
		return  (2*nEdges)/(nVertices*(nVertices-1));
	}
	public Dataset<Row> trianglesCounter(){
		return graph.triangleCount().run();
	}
	//CENTRALITY MEASURES

	//DEGREES
	public Dataset<Row> degrees(){
		return graph.degrees();
	}
	public Dataset<Row> degrees(String condition){
		return graph.degrees().filter(condition);
	}

	//CLOSENESS
	public Dataset<Row> closeness(ArrayList<Object> landmarks){
		Dataset<Row> paths=graph.shortestPaths().landmarks(landmarks).run();
		Dataset<Row> explodedPaths=paths
				.select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")));
		Dataset<Row> t=explodedPaths.groupBy("key").sum("value");
		return 	t.withColumn("sum(value)",org.apache.spark.sql.functions.pow(t.col("sum(value)"),-1));
	}


	public GraphFrame subGraph(String condition,boolean vertices){
		if(vertices){
			return graph.filterVertices(condition);}
		else{
			return graph.filterEdges(condition).dropIsolatedVertices();
		}
	}

	public GraphFrame subGraph(String conditionV,String conditionE){
		return graph.filterVertices(conditionV).filterEdges(conditionE).dropIsolatedVertices();
	}

	public Dataset<Row> shortestPath(ArrayList<Object> landmarks){
		Dataset<Row> paths=graph.shortestPaths().landmarks(landmarks).run();
		Dataset<Row> explodedPaths=paths
				.select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")));
		return explodedPaths;
	}

	public GraphFrame filterByDegree(GraphFrame G,int x){
		Dataset<Row> id_to_degree=G.degrees().filter("degree>"+x);
		Dataset<Row> edges=G.edges().join(id_to_degree,G.edges().col("src").equalTo(id_to_degree.col("id")));
		edges=edges.withColumnRenamed("id", "id1").withColumnRenamed("degree","d1");
		edges=edges.join(id_to_degree,edges.col("dst").equalTo(id_to_degree.col("id")));
		GraphFrame filtered=GraphFrame.fromEdges(edges);
		return filtered;
	}

	public Dataset<Row> connectedComponents(){
		Dataset<Row> components=graph.connectedComponents().run();
		return components;
	}

}
