package ppispark.connectedComponents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import org.neo4j.spark.Neo4j;
import scala.Function2;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;

public class GraphManager {
	public SparkSession spark;
	public String inputPath;
	public GraphFrame graph;
	public GraphManager(SparkSession spark, String inputPath){
		this.spark=spark;
		this.inputPath=inputPath;
		graph=fromTsv(inputPath);
		spark.sparkContext().setCheckpointDir("PPI-Check");
	}
	public GraphManager(SparkSession spark, String inputPath,String CheckPath){
		this.spark=spark;
		this.inputPath=inputPath;
		graph=fromTsv(inputPath);
		spark.sparkContext().setCheckpointDir(CheckPath);
	}


	//LOAD GRAPH FROM TSV FILE
	public GraphFrame fromTsv(String path) {
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

	
	//EXPORT GRAPH TO TSV TEXT FILE
	public void toTsv() {}
	
	//LOAD GRAPH FROM NEO4J
	public GraphFrame fromNeo4j(String[] edgesLabels) {
		Neo4j conn = new Neo4j(spark.sparkContext());
		String s="";
		for(String x:edgesLabels){
			s+=",r."+x+" AS "+x;
		}
//		Dataset<Row> d = conn.cypher("MATCH (a)-[r]->(b) RETURN a.name AS src,b.name AS dst,r.alt_id_A AS alt_id_A,r.alt_id_B AS alt_id_B,r.alias_A AS alias_A,r.alias_B AS alias_B,r.det_method AS det_method,r.first_auth AS first_auth,r.id_pub AS id_pub,r.ncbi_id_A AS ncbi_id_A,r.ncbi_id_B AS ncbi_id_B,r.int_types AS int_types,r.source_db AS source_db,r.int_id AS int_id,r.conf_score AS conf_score,r.comp_exp AS comp_exp,r.bio_role_A AS bio_role_A,r.bio_role_B AS bio_role_B,r.exp_role_A AS exp_role_A,r.exp_role_B AS exp_role_B,r.type_A AS type_A,r.type_B AS type_B,r.xref_A AS xref_A,r.xref_B AS xref_B,r.xref_int AS xref_int,r.annot_A AS annot_A,r.annot_B AS annot_B,r.annot_int AS annot_int,r.ncbi_id_organism AS ncbi_id_organism,r.param_int AS param_int,r.create_data AS create_data,r.up_date AS up_date,r.chk_A AS chk_A,r.chk_B AS chk_B,r.chk_int AS chk_int,r.negative AS negative,r.feat_A AS feat_A,r.feat_B AS feat_B,r.stoich_A AS stoich_A,r.stoich_B AS stoich_B,r.part_meth_A AS part_meth_A,r.part_meth_B AS part_meth_B",JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadDataFrame();
		Dataset<Row> d = conn.cypher("MATCH (a)-[r]->(b) RETURN a.name AS src,b.name AS dst"+s,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadDataFrame();
		GraphFrame graph = GraphFrame.fromEdges(d);
		return graph;
	}
	
	//EXPORT GRAPH TO NEO4J
	public void toNeo4j(GraphFrame graph,SparkSession spark) {
		ClassTag<Row> row_tag = scala.reflect.ClassTag$.MODULE$.apply(Row.class);
		Neo4j conn = new Neo4j(spark.sparkContext());

		// todo: to check, before: conn.saveGraph(graph.toGraphX(), conn.saveGraph$default$2(), conn.saveGraph$default$3(), true, row_tag, row_tag);
		conn.saveGraph(graph.toGraphX(), "PPINetwork", null, true, row_tag, row_tag);
	}

	public Dataset<Row> vertices(){
		Dataset<Row> v=graph.vertices();
		System.out.println("Number of vertices: "+v.count());
		return v;
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

	public Dataset<Row> degrees(GraphFrame g,String condition){
		return g.degrees().filter(condition);
	}
	public Dataset<Row> triangle(){
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
	public GraphFrame filterByNeighbors( ArrayList<Object> N, int x,boolean b){

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

	public void componentsIntersection(GraphFrame graph, ArrayList<Object> N) throws IOException {
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




}
