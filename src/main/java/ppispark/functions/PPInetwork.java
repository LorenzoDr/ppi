package ppispark.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.graphframes.GraphFrame;

import org.neo4j.spark.Neo4j;
import ppiscala.graphUtil;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

public class PPInetwork {

	public SparkSession spark;
	public String inputPath;
	public GraphFrame graph;


	public PPInetwork(SparkSession spark, String inputPath){
		this.spark=spark;
		this.inputPath=inputPath;
		graph=importFromTsv(inputPath);
		spark.sparkContext().setCheckpointDir("PPI-Check");
	}

	public PPInetwork(SparkSession spark, String inputPath, String CheckPath){
		this.spark=spark;
		this.inputPath=inputPath;
		graph=importFromTsv(inputPath);
		spark.sparkContext().setCheckpointDir(CheckPath);
	}


	//LOAD GRAPH FROM TSV FILE
	public GraphFrame importFromTsv(String path) {
			String[] cols_names = new String[] { "src", "dst", "alt_id_A", "alt_id_B", "alias_A", "alias_B", "det_method",
					"first_auth", "id_pub", "ncbi_id_A", "ncbi_id_B", "int_types", "source_db", "int_id", "conf_score",
					"comp_exp", "bio_role_A", "bio_role_B", "exp_role_A", "exp_role_B", "type_A", "type_B", "xref_A",
					"xref_B", "xref_int", "annot_A", "annot_B", "annot_int", "ncbi_id_organism", "param_int", "create_data",
					"up_date", "chk_A", "chk_B", "chk_int", "negative", "feat_A", "feat_B", "stoich_A", "stoich_B",
					"part_meth_A", "part_meth_B" };
		
			Dataset<Row> edges = spark.read().
	                option("header", "True").
	                option("sep", "\t"). //delimiter?
	                csv(path);

			
	        edges = edges.toDF(cols_names);//.withColumn("new_col",functions.lit(1));
	        GraphFrame graph = GraphFrame.fromEdges(edges);
	        return graph;
    }

	
	//LOAD GRAPH FROM NEO4J
	public GraphFrame importGraphFromNeo4j(String url, String user, String password, String[] nodesLabels, String[] edgesProperties) {
		spark.sparkContext().conf()//.set("spark.neo4j.encryption.status","false")
				.set("spark.neo4j.url", url)
				.set("spark.neo4j.user", user)
				.set("spark.neo4j.password", password);

		StructType schemaVertices=new StructType().add("id","String");
		StructType schemaEdges=new StructType().add("src","String").add("dst","String");

		String nodesString="";
		String edgesString="";
		for(int i=1;i<nodesLabels.length;i++){
			nodesString+=",toString(a."+nodesLabels[i]+")";
			schemaVertices=schemaVertices.add(nodesLabels[i],"String");
		}
		for(int i=0;i<edgesProperties.length;i++){
			edgesString+=",toString(r."+edgesProperties[i]+")";
			schemaEdges=schemaEdges.add(edgesProperties[i],"String");
		}
		Neo4j conn = new Neo4j(spark.sparkContext());


		RDD<Row> rddVertices=conn.cypher("MATCH (a) RETURN toString(a."+nodesLabels[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
		RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) RETURN toString(a."+nodesLabels[0]+"),toString(b."+nodesLabels[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



		Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
		Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);

		GraphFrame output=GraphFrame.apply(vertices,edges);

		return output;
	}


	//filter nodes while importing
	public GraphFrame filteredNodesfromNeo4j(String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] conditions) {

		spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
				.set("spark.neo4j.url", "bolt://localhost:7687")
				.set("spark.neo4j.user", "neo4j")
				.set("spark.neo4j.password", "Cirociro94");

		StructType schemaVertices=new StructType().add("id","String");
		StructType schemaEdges=new StructType().add("src","String").add("dst","String");

		String nodesString="";
		String edgesString="";
		String filter1="";
		String filter2="";

		for(int i=1;i<nodesLabels.length;i++){
				nodesString+=",toString(a."+nodesLabels[i]+")";
				schemaVertices=schemaVertices.add(nodesLabels[i],"String");
		}
		for(int i=1;i<conditions.length;i++){
				filter1+="AND a."+conditions[i];
				filter2+="AND a."+conditions[i];
				filter2+="AND b."+conditions[i];
		}
		for(int i=0;i<edgesProperties.length;i++){
				edgesString+=",toString(r."+edgesProperties[i]+")";
				schemaEdges=schemaEdges.add(edgesProperties[i],"String");
		}
		Neo4j conn = new Neo4j(spark.sparkContext());


		RDD<Row> rddVertices=conn.cypher("MATCH (a) WHERE a."+conditions[0]+" "+filter1+" RETURN toString(a."+nodesLabels[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
		RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE a."+conditions[0]+" AND b."+conditions[0]+" "+filter2+" RETURN toString(a."+nodesLabels[0]+"),toString(b."+nodesLabels[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



		Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
		Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);


		GraphFrame output=GraphFrame.apply(vertices,edges);

		return output;
	}

	//filter edges while importing
	public GraphFrame filteredEdgesfromNeo4j(String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] conditions,boolean dropVertices) {

		spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
				.set("spark.neo4j.url", "bolt://localhost:7687")
				.set("spark.neo4j.user", "neo4j")
				.set("spark.neo4j.password", "Cirociro94");

		StructType schemaVertices=new StructType().add("id","String");
		StructType schemaEdges=new StructType().add("src","String").add("dst","String");

		String nodesString="";
		String edgesString="";
		String filter="";


		for(int i=1;i<nodesLabels.length;i++){
			nodesString+=",toString(a."+nodesLabels[i]+")";
			schemaVertices=schemaVertices.add(nodesLabels[i],"String");
		}
		for(int i=1;i<conditions.length;i++){
			filter+="AND r."+conditions[i];
		}
		for(int i=0;i<edgesProperties.length;i++){
			edgesString+=",toString(r."+edgesProperties[i]+")";
			schemaEdges=schemaEdges.add(edgesProperties[i],"String");
		}
		Neo4j conn = new Neo4j(spark.sparkContext());


		RDD<Row> rddVertices=conn.cypher("MATCH (a) RETURN toString(a."+nodesLabels[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
		RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE r."+conditions[0]+filter+" RETURN toString(a."+nodesLabels[0]+"),toString(b."+nodesLabels[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



		Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
		Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);


		GraphFrame output=GraphFrame.apply(vertices,edges);

		if(dropVertices){
			return output.dropIsolatedVertices();
		}else{
			return output;
		}

	}

	public GraphFrame filteredEdgesfromNeo4j(String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] conditions){
		return filteredEdgesfromNeo4j(url,user,password,nodesLabels,edgesProperties,conditions,true);
	}

	//filter Nodes and Edges while importing
	public GraphFrame importGraphfromNeo4j(String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] nodesCondition,String[] edgesCondition,boolean dropVertices) {

		spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
				.set("spark.neo4j.url", "bolt://localhost:7687")
				.set("spark.neo4j.user", "neo4j")
				.set("spark.neo4j.password", "Cirociro94");

		StructType schemaVertices=new StructType().add("id","String");
		StructType schemaEdges=new StructType().add("src","String").add("dst","String");

		String nodesString="";
		String edgesString="";
		String filter1="";
		String filter2="";

		for(int i=1;i<nodesLabels.length;i++){
			nodesString+=",toString(a."+nodesLabels[i]+")";
			schemaVertices=schemaVertices.add(nodesLabels[i],"String");
		}
		for(int i=1;i<nodesCondition.length;i++){
			filter1+="AND a."+nodesCondition[i];
			filter2+="AND a."+nodesCondition[i];
			filter2+="AND b."+nodesCondition[i];
		}
		for(int i=0;i<edgesProperties.length;i++){
			edgesString+=",toString(r."+edgesProperties[i]+")";
			schemaEdges=schemaEdges.add(edgesProperties[i],"String");
		}
		for(int i=0;i<edgesCondition.length;i++){
			filter2+="AND r."+edgesCondition[i];
		}

		Neo4j conn = new Neo4j(spark.sparkContext());


		RDD<Row> rddVertices=conn.cypher("MATCH (a) WHERE a."+nodesCondition[0]+" "+filter1+" RETURN toString(a."+nodesLabels[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
		RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE a."+nodesCondition[0]+" AND b."+nodesCondition[0]+" "+filter2+" RETURN toString(a."+nodesLabels[0]+"),toString(b."+nodesLabels[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



		Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
		Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);

		GraphFrame output=GraphFrame.apply(vertices,edges);

		if(dropVertices){
			return output.dropIsolatedVertices();
		}
		else{
			return output;
		}

	}

	public GraphFrame importGraphfromNeo4j(String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] nodesCondition,String[] edgesCondition){
		return importGraphfromNeo4j(url,user,password,nodesLabels,edgesProperties,nodesCondition,edgesCondition,true);
	}

	public Dataset<Row> vertices(){
		Dataset<Row> v=graph.vertices();
		System.out.println("Number of vertices: "+v.count());
		return v;
	}

	public long verticesCounter(){
		return graph.vertices().count();
	}

	public Dataset<Row> vertices(GraphFrame g){
		Dataset<Row> v=g.vertices();
		System.out.println("Number of vertices: "+v.count());
		return v;
	}

	public Dataset<Row> vertices(String condition){
		Dataset<Row> v=graph.vertices();
		System.out.println("Number of vertices: "+v.count());
		return v.filter(condition);
	}

	public Dataset<Row> vertices(GraphFrame g,String condition){
		Dataset<Row> v=g.vertices();
		System.out.println("Number of vertices: "+v.count());
		return v.filter(condition);
	}

	public Dataset<Row> edges(){
		Dataset<Row> e=graph.edges();
		System.out.println("Number of edges: "+e.count());
		return e;
	}

	public long edgesCounter(){
		return graph.edges().count();
	}

	public long density(){
		long nVertices=graph.vertices().count();
		long nEdges=graph.edges().count();
		return (2*nEdges)/(nVertices*(nVertices-1));
	}

	public Dataset<Row> edges(GraphFrame g){
		Dataset<Row> e=g.edges();
		System.out.println("Number of edges: "+e.count());
		return e;
	}

	public Dataset<Row> edges(String condition){
		Dataset<Row> e=graph.edges();
		System.out.println("Number of edges: "+e.count());
		return e.filter(condition);
	}

	public Dataset<Row> edges(GraphFrame g,String condition){
		Dataset<Row> e=g.edges();
		System.out.println("Number of edges: "+e.count());
		return e.filter(condition);
	}

	public Dataset<Row> degrees(){
		return graph.degrees();
	}
	public Dataset<Row> degrees(GraphFrame g){
		return g.degrees();
	}
	public Dataset<Row> degrees(String condition){
		return graph.degrees().filter(condition);
	}

	//Centrality index
	public Dataset<Row> closeness(ArrayList<Object> landmarks){
		Dataset<Row> paths=graph.shortestPaths().landmarks(landmarks).run();
		Dataset<Row> explodedPaths=paths
				.select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")));
		Dataset<Row> t=explodedPaths.groupBy("key").sum("value");
		return 	t.withColumn("sum(value)",org.apache.spark.sql.functions.pow(t.col("sum(value)"),-1));
	}

	public Dataset<Row> degrees(GraphFrame g,String condition){
		return g.degrees().filter(condition);
	}
	public Dataset<Row> trianglesCounter(){
		return graph.triangleCount().run();
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
	//CONNECTED COMPONENT WITH LIST
	public GraphFrame connectedComponent(GraphFrame graph,int degree,SparkSession spark,String CheckPath,List<String> N){
		  Dataset<Row> id_to_degree=graph.degrees().filter("degree>"+degree);  
		  Dataset<Row> edges=graph.edges().join(id_to_degree,graph.edges().col("src").equalTo(id_to_degree.col("id")));
		  edges=edges.withColumnRenamed("id", "id1").withColumnRenamed("degree","d1");
		  edges=edges.join(id_to_degree,edges.col("dst").equalTo(id_to_degree.col("id")));
		  
		  spark.sparkContext().setCheckpointDir(CheckPath);
		  GraphFrame filtered=GraphFrame.fromEdges(edges);
		  Dataset<Row> components=filtered.connectedComponents().run();
	
		  
		  Tuple2<Long, Integer> max=components.javaRDD()
				  .mapToPair(r->new Tuple2<>(Long.parseLong(r.get(1).toString()),(String)r.get(0).toString()))
				  .mapToPair(new Ncounter(N))
				  .reduceByKey((i1,i2)->{return i1+i2;})
				  .max(new comparator());
				
		  Dataset<Row> maxComponent=components.filter("component="+max._1);
			 
	          edges=edges.withColumnRenamed("id", "id2");
	          edges.join(maxComponent,edges.col("src").equalTo(maxComponent.col("id"))).show();
		  GraphFrame output=GraphFrame.fromEdges(edges);
			 
		  return output;
		
	}


	
	// INTERSECTIONS BETWEEN COMPONENTS AND THE LIST N
	public void componentsIntersection(GraphFrame graph,SparkSession spark,String CheckPath,List<String> N,int degree) throws IOException {
		  
		  if(degree>=0) {
			  Dataset<Row> id_to_degree=graph.degrees().filter("degree>"+degree);  
			  Dataset<Row> edges=graph.edges().join(id_to_degree,graph.edges().col("src").equalTo(id_to_degree.col("id")));
			  edges=edges.withColumnRenamed("id", "id1").withColumnRenamed("degree","d1");
			  edges=edges.join(id_to_degree,edges.col("dst").equalTo(id_to_degree.col("id")));
			  graph=GraphFrame.fromEdges(edges);
		  }
		  
		  spark.sparkContext().setCheckpointDir(CheckPath);
		  Dataset<Row> components=graph.connectedComponents().run();
	
		  
		  JavaPairRDD<Long, Integer> intersections=components.javaRDD()
				  .mapToPair(r->new Tuple2<>(Long.parseLong(r.get(1).toString()),(String)r.get(0).toString()))
				  .mapToPair(new Ncounter(N))
				  .reduceByKey((i1,i2)->{return i1+i2;});
				  //.zipWithIndex()
		          //.mapToPair(t->new Tuple2<>(t._2,t._1._2));
		  
		 FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
		 FSDataOutputStream out = fs.create(new Path("output.csv"));
		 out.writeBytes("Component-ID,N"+ "\n");
		 for(Tuple2<Long,Integer> t:intersections.collect()) {
			  out.writeBytes(t._1+","+t._2+ "\n");
		 }
		 out.close();
		 	
	}
	
	
	public void componentsIntersection(GraphFrame graph,SparkSession spark,String CheckPath,List<String> N) throws IOException{
		 componentsIntersection(graph,spark,CheckPath,N,-1);
	}
	
	//CONNECTED COMPONENT WITH SUBGRAPH
	public GraphFrame connectedComponent(GraphFrame graph,int degree,SparkSession spark,String CheckPath,GraphFrame N){
		 Dataset<Row> id_to_degree=graph.degrees().filter("degree>"+degree);  
		 Dataset<Row> edges=graph.edges().join(id_to_degree,graph.edges().col("src").equalTo(id_to_degree.col("id")));
		 edges=edges.withColumnRenamed("id", "id1").withColumnRenamed("degree","d1");
		 edges=edges.join(id_to_degree,edges.col("dst").equalTo(id_to_degree.col("id")));
		  
		 spark.sparkContext().setCheckpointDir(CheckPath);
		 GraphFrame filtered=GraphFrame.fromEdges(edges);
		 Dataset<Row> components=filtered.connectedComponents().run();
		 
		 Tuple2<Long, Integer> max=components.join(N.vertices(), "id")
				  .toJavaRDD()
				  .mapToPair(r->new Tuple2<>(Long.parseLong(r.get(1).toString()),(String)r.get(0).toString()))
				  .mapToPair(t->new Tuple2<>(t._1,1))
				  .reduceByKey((i1,i2)->{return i1+i2;})
				  .max(new comparator());
		 Dataset<Row> maxComponent=components.filter("component="+max._1);
		 
		 edges=edges.withColumnRenamed("id", "id2");
		 edges.join(maxComponent,edges.col("src").equalTo(maxComponent.col("id"))).show();
		 GraphFrame output=GraphFrame.fromEdges(edges);

		 return	output;
	}
	
	// INTERSECTIONS BETWEEN COMPONENTS AND THE SUBGRAPH N
	public void componentsIntersection(GraphFrame graph,SparkSession spark,String CheckPath,GraphFrame N,int degree) throws IOException{
		  
		  if(degree>=0) {
			  Dataset<Row> id_to_degree=graph.degrees().filter("degree>"+degree);  
			  Dataset<Row> edges=graph.edges().join(id_to_degree,graph.edges().col("src").equalTo(id_to_degree.col("id")));
			  edges=edges.withColumnRenamed("id", "id1").withColumnRenamed("degree","d1");
			  edges=edges.join(id_to_degree,edges.col("dst").equalTo(id_to_degree.col("id")));
			  graph=GraphFrame.fromEdges(edges);
		  }
		  
		 System.out.println(graph.vertices().count());
		 spark.sparkContext().setCheckpointDir(CheckPath);
		 Dataset<Row> components=graph.connectedComponents().run();
	
		  
	         JavaPairRDD<Long,Integer> intersections=components.join(N.vertices(), "id")
					  .toJavaRDD()
					  .mapToPair(r->new Tuple2<>(Long.parseLong(r.get(1).toString()),(String)r.get(0).toString()))
					  .mapToPair(t->new Tuple2<>(t._1,1))
					  .reduceByKey((i1,i2)->{return i1+i2;});
		  
		
		  
		 FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
	         FSDataOutputStream out = fs.create(new Path("output.csv"));
	         out.writeBytes("Component-ID,N"+ "\n");
		 for(Tuple2<Long,Integer> t:intersections.collect()) {
			  out.writeBytes(t._1+","+t._2+ "\n");
		 }
		 out.close();
	}
	
	public void componentsIntersection(GraphFrame graph,SparkSession spark,String CheckPath,GraphFrame N) throws IOException{
		 componentsIntersection(graph,spark,CheckPath,N,-1);
	}

	//F4
	public GraphFrame filterByNeighbors(ArrayList<Object> N, int x,boolean b){

		if(b){
			Dataset<Row> paths=graph.shortestPaths().landmarks(N).run();
			Dataset<Row> explodedPaths=paths
					.select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")))
					.filter("value<="+x)
					.drop("value")
					.groupBy("id")
					.agg(org.apache.spark.sql.functions.collect_list("key").as("key"));
			Dataset<Row> edges=graph.edges()
					.join(explodedPaths,graph.edges().col("src").equalTo(explodedPaths.col("id")));
			edges=edges
					.withColumnRenamed("id", "id1")
					.withColumnRenamed("key","x_src");
			edges=edges.join(explodedPaths,edges.col("dst").equalTo(explodedPaths.col("id")));
			edges=edges.withColumnRenamed("key", "x_dst").drop("id").drop("id1");
			graph=GraphFrame.fromEdges(edges);

			return graph;
		}
		else{
			Dataset<Row> paths=graph.shortestPaths().landmarks(N).run();
			Dataset<Row> explodedPaths=paths
					.select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")))
					.filter("value<="+x)
					.drop("key")
					.drop("value")
					.distinct();
			Dataset<Row> edges=graph.edges().join(explodedPaths,graph.edges().col("src").equalTo(explodedPaths.col("id")));
			edges=edges.withColumnRenamed("id", "id1");
			edges=edges.join(explodedPaths,edges.col("dst").equalTo(explodedPaths.col("id")));
			edges=edges.withColumnRenamed("id", "id2");
			graph=GraphFrame.fromEdges(edges);
			return graph;
		}

	}

	//F1
	public GraphFrame filterComponents(ArrayList<Object> N, int x){
		if(x > 0){
			graph=filterByNeighbors(N,x,false);
		}
		Dataset<Row> components=graph.connectedComponents().run();
		Tuple2<Long, Integer> max=components.javaRDD()
				.mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
				.mapToPair(new Ncounter2(N))
				.reduceByKey((i1,i2)->{return i1+i2;})
				.max(new comparator());
		Dataset<Row> maxComponent=components.filter("component="+max._1);
		Dataset<Row> filteredEdges=graph.edges().join(maxComponent,graph.edges().col("src").equalTo(maxComponent.col("id")));
		GraphFrame output=GraphFrame.fromEdges(filteredEdges);
		return output;
	}

	public GraphFrame provaF1(ArrayList<Object> N, int x){
		Dataset<Row> components=graph.connectedComponents().run();

		Tuple2<Long, Integer> max=components.javaRDD()
				.mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
				.mapToPair(new Ncounter2(N))
				.reduceByKey((i1,i2)->{return i1+i2;})
				.max(new comparator());
		Dataset<Row> maxComponent=components.filter("component="+max._1);
		Dataset<Row> filteredEdges=graph.edges().join(maxComponent,graph.edges().col("src").equalTo(maxComponent.col("id")));
		filteredEdges=filteredEdges.drop("id").drop("component");
		filteredEdges=filteredEdges.join(maxComponent,filteredEdges.col("dst").equalTo(maxComponent.col("id")));
		GraphFrame output=GraphFrame.fromEdges(filteredEdges);
		if(x > 0){
			output=filterByNeighbors(N,x,false);
		}
		return output;

	}

	public GraphFrame filterComponents(ArrayList<Object> N){
		return filterComponents(N,0);
	}

	//F2
	public void componentsIntersection(ArrayList<Object> N,int x) throws IOException {

		if(x>0) {
			graph=filterByNeighbors(N,x,false);
		}

		Dataset<Row> components=graph.connectedComponents().run();


		JavaPairRDD<Long, Integer> intersections=components.javaRDD()
				.mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
				.mapToPair(new Ncounter2(N))
				.reduceByKey((i1,i2)->{return i1+i2;});


		FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
		FSDataOutputStream out = fs.create(new Path("output.csv"));
		out.writeBytes("Component-ID,N"+ "\n");
		for(Tuple2<Long,Integer> t:intersections.collect()) {
			if(t._2>0){
			 out.writeBytes(t._1+","+t._2+ "\n");}
		}
		out.close();
	}

	public void componentsIntersection(ArrayList<Object> N) throws IOException {
		componentsIntersection(N,0);
	}

	//F3
	public Dataset<Row> xNeighbors(String id, int x){
		ArrayList<Object> landmarks=new ArrayList<Object>();
		landmarks.add(id);
		Dataset<Row> shortestPaths=graph.shortestPaths().landmarks(landmarks).run();
		Dataset<Row> output=shortestPaths
				.select(shortestPaths.col("id"),org.apache.spark.sql.functions.explode(shortestPaths.col("distances")))
				.filter("value<="+x)
				.drop("key")
				.drop("value");
		return output;
	}

	public Dataset<Row> F5(String inputNode, int x){

		Dataset<Row> edges=graph.edges().withColumn("weight", org.apache.spark.sql.functions.lit(-1));
		GraphFrame graph1=GraphFrame.fromEdges(edges);
		Dataset<Row> weightedPath= graphUtil.maxWeightedPaths(graph1,inputNode,spark);
		return weightedPath.filter("weight>="+x+" OR weight==0");
	}

	public GraphFrame F6(String inputNode, int x){
		Dataset<Row> xNeighbors=F5(inputNode,x);
		Dataset<Row> edges=graph.edges();
		edges=edges.join(xNeighbors,edges.col("src").equalTo(xNeighbors.col("id")));
		edges=edges.drop("id").drop("weight");
		edges=edges.join(xNeighbors,edges.col("dst").equalTo(xNeighbors.col("id")));
		return GraphFrame.fromEdges(edges);
	}

	public GraphFrame F7(ArrayList<Object> input, int x){
		Dataset<Row> edges=graph.edges().withColumn("weight", org.apache.spark.sql.functions.lit(-1));
		GraphFrame graph1=GraphFrame.fromEdges(edges);
		Dataset<Row> xNeighbors=graphUtil.maxWeightedPaths(graph1,input,spark);
		Dataset<Row> vertices=xNeighbors.filter("weight>="+x+" OR weight==0")
				.groupBy("id")
				.agg(org.apache.spark.sql.functions.collect_list("key").as("key"));
		edges=graph1.edges();
		edges=edges.join(vertices,edges.col("src").equalTo(vertices.col("id")));
		edges=edges.drop("id").drop("key");
		edges=edges.join(vertices,edges.col("dst").equalTo(vertices.col("id")));
		GraphFrame output=graphUtil.toGraphFrame(spark,vertices,edges);
		return output;

	}

	public GraphFrame F8(ArrayList<ArrayList<Object>> provaInput, int x){
		List<String> data = Arrays.asList("A","B","C","D","E");
		SparkContext sparkContext=new SparkContext();
		Dataset<Row> edges=graph.edges().withColumn("weight", org.apache.spark.sql.functions.lit(-1));
		GraphFrame graph1=GraphFrame.fromEdges(edges);
		StructType s = new StructType()
				.add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("key", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("weight", DataTypes.DoubleType, true, Metadata.empty()));

		Dataset<Row> xNeighbors = spark.read().schema(s).csv(spark.emptyDataset(Encoders.STRING()));

		for(int i=0;i< provaInput.size();i++){
			Dataset<Row> tmp=graphUtil.maxWeightedPaths(graph1, provaInput.get(i),i,spark);
			xNeighbors=xNeighbors.union(tmp);
		}
		Dataset<Row> vertices=xNeighbors.filter("weight>="+x+" OR weight==0")
				.groupBy("id")
				.agg(org.apache.spark.sql.functions.collect_list("key").as("key"));
		edges=graph1.edges();
		edges=edges.join(vertices,edges.col("src").equalTo(vertices.col("id")));
		edges=edges.drop("id").drop("key");
		edges=edges.join(vertices,edges.col("dst").equalTo(vertices.col("id")));
		GraphFrame output=graphUtil.toGraphFrame(spark,vertices,edges);
		return output;

	}

	public GraphFrame F9(ArrayList<ArrayList<Object>> input, int x){

		JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
		JavaRDD<Row> N=jsc.emptyRDD();


		for(int i=0;i<input.size();i++){
			int y=i+1;
			JavaRDD<Row> tmp=jsc.parallelize(input.get(i)).map(t->{
				Object[] o=new Object[]{t, y};
				Row r=RowFactory.create(o);
				return r;
			});
			N=N.union(tmp);
		}

		StructType s = new StructType()
				.add(new StructField("id1", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("N", DataTypes.IntegerType, true, Metadata.empty()));

		Dataset<Row> prova=spark.createDataFrame(N, s).groupBy("id1")
				.agg(org.apache.spark.sql.functions.collect_list("N").as("N"));;
		prova.show();

		GraphFrame g=F7(input.get(0),x);
		Dataset<Row> vertices=g.vertices();
		vertices=vertices.join(prova,vertices.col("id")
				.equalTo(prova.col("id1")),"left")
				.drop("id1");
		return graphUtil.toGraphFrame(spark,vertices,g.edges());
	}



}
