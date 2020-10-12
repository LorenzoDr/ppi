package ppispark.functions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.graphframes.GraphFrame;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
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


	//TSV FILE
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

	public void exportToTsv(GraphFrame g,String filename) {
		spark.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		spark.sparkContext().hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

		graph.edges().coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").csv("output");
		File f = new File("output");

		for(String s:f.list()){
			if(s.endsWith("crc")){
				File file=new File("output/"+s);
				file.delete();
			}else{
				File file=new File("output/"+s);
				File file1=new File(filename);
				file.renameTo(file1);
				f.delete();
			}
		}
	}

	//MongoDB
	public GraphFrame importEdgesFromMongoDB(String uri, String src, String dst) {
		SparkContext sc=spark.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);
		jsc.sc().conf()
				.set("spark.mongodb.input.uri", uri)
				.set("spark.mongodb.output.uri", uri);

		Dataset<Row> edges = MongoSpark.load(jsc).toDF();

		edges=edges.drop("_id");
		edges=edges.withColumnRenamed(src,"src");
		edges=edges.withColumnRenamed(dst,"dst");

		GraphFrame g=GraphFrame.fromEdges(edges);
		return g;
	}

	public GraphFrame importGraphFromMongoDB(String edgesUri,String nodesUri,String id,String src,String dst) {
		SparkContext sc=spark.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);

		jsc.sc().conf()
				.set("spark.mongodb.input.uri", edgesUri)
				.set("spark.mongodb.output.uri", edgesUri);
		Dataset<Row> edges = MongoSpark.load(jsc).toDF();
		edges=edges.drop("_id");
		edges=edges.withColumnRenamed(src,"src");
		edges=edges.withColumnRenamed(dst,"dst");

		jsc.sc().conf()
				.set("spark.mongodb.input.uri", nodesUri)
				.set("spark.mongodb.output.uri", nodesUri);
		Dataset<Row> nodes= MongoSpark.load(jsc).toDF();
		nodes=nodes.drop("_id");
		nodes=nodes.withColumnRenamed(id,"id");

		return GraphFrame.apply(nodes,edges);
	}
	public void toMongoDB(String uri,String collection){
		SparkContext sc=spark.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);

		jsc.sc().conf()
				.set("spark.mongodb.input.uri", uri)
				.set("spark.mongodb.output.uri",uri);
		MongoSpark.write(graph.edges()).option("collection", collection).mode("overwrite").save();
	}

	public void toMongoDB(String uri,String edgesCollection,String nodesCollection){
		SparkContext sc=spark.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);

		jsc.sc().conf()
				.set("spark.mongodb.input.uri", uri)
				.set("spark.mongodb.output.uri",uri);
		MongoSpark.write(graph.edges()).option("collection", edgesCollection).mode("overwrite").save();
		MongoSpark.write(graph.edges()).option("collection", nodesCollection).mode("overwrite").save();

	}

	//LOAD GRAPH FROM NEO4J
	public GraphFrame importGraphFromNeo4j(String url, String user, String password, String[] nodesProperties, String[] edgesProperties) {
		spark.sparkContext().conf()//.set("spark.neo4j.encryption.status","false")
				.set("spark.neo4j.url", url)
				.set("spark.neo4j.user", user)
				.set("spark.neo4j.password", password);

		StructType schemaVertices=new StructType().add("id","String");
		StructType schemaEdges=new StructType().add("src","String").add("dst","String");

		String nodesString="";
		String edgesString="";
		for(int i=1;i<nodesProperties.length;i++){
			nodesString+=",toString(a."+nodesProperties[i]+")";
			schemaVertices=schemaVertices.add(nodesProperties[i],"String");
		}
		for(int i=0;i<edgesProperties.length;i++){
			edgesString+=",toString(r."+edgesProperties[i]+")";
			schemaEdges=schemaEdges.add(edgesProperties[i],"String");
		}
		Neo4j conn = new Neo4j(spark.sparkContext());


		RDD<Row> rddVertices=conn.cypher("MATCH (a) RETURN toString(a."+nodesProperties[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
		RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) RETURN toString(a."+nodesProperties[0]+"),toString(b."+nodesProperties[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



		Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
		Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);

		GraphFrame output=GraphFrame.apply(vertices,edges);


		return output;
	}


	//FILTER NODES WHILE IMPORTING FROM NEO4J
	public GraphFrame filteredNodesFromNeo4j(String url, String user, String password, String[] nodesProperties, String[] edgesProperties,String[] conditions) {

		spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
				.set("spark.neo4j.url", url)
				.set("spark.neo4j.user", user)
				.set("spark.neo4j.password", password);

		StructType schemaVertices=new StructType().add("id","String");
		StructType schemaEdges=new StructType().add("src","String").add("dst","String");

		String nodesString="";
		String edgesString="";
		String filter1="";
		String filter2="";

		for(int i=1;i<nodesProperties.length;i++){
				nodesString+=",toString(a."+nodesProperties[i]+")";
				schemaVertices=schemaVertices.add(nodesProperties[i],"String");
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


		RDD<Row> rddVertices=conn.cypher("MATCH (a) WHERE a."+conditions[0]+" "+filter1+" RETURN toString(a."+nodesProperties[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
		RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE a."+conditions[0]+" AND b."+conditions[0]+" "+filter2+" RETURN toString(a."+nodesProperties[0]+"),toString(b."+nodesProperties[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



		Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
		Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);


		GraphFrame output=GraphFrame.apply(vertices,edges);

		return output;
	}

	//FILTER EDGES WHILE IMPORTING
	public GraphFrame filteredEdgesFromNeo4j(String url, String user, String password, String[] nodesProperties, String[] edgesProperties,String[] conditions,boolean dropVertices) {

		spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
				.set("spark.neo4j.url", url)
				.set("spark.neo4j.user", user)
				.set("spark.neo4j.password", password);

		StructType schemaVertices=new StructType().add("id","String");
		StructType schemaEdges=new StructType().add("src","String").add("dst","String");

		String nodesString="";
		String edgesString="";
		String filter="";


		for(int i=1;i<nodesProperties.length;i++){
			nodesString+=",toString(a."+nodesProperties[i]+")";
			schemaVertices=schemaVertices.add(nodesProperties[i],"String");
		}
		for(int i=1;i<conditions.length;i++){
			filter+="AND r."+conditions[i];
		}
		for(int i=0;i<edgesProperties.length;i++){
			edgesString+=",toString(r."+edgesProperties[i]+")";
			schemaEdges=schemaEdges.add(edgesProperties[i],"String");
		}
		Neo4j conn = new Neo4j(spark.sparkContext());


		RDD<Row> rddVertices=conn.cypher("MATCH (a) RETURN toString(a."+nodesProperties[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
		RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE r."+conditions[0]+filter+" RETURN toString(a."+nodesProperties[0]+"),toString(b."+nodesProperties[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();



		Dataset<Row> vertices=spark.createDataFrame(rddVertices.toJavaRDD(),schemaVertices);
		Dataset<Row> edges=spark.createDataFrame(rddEdges.toJavaRDD(),schemaEdges);


		GraphFrame output=GraphFrame.apply(vertices,edges);

		if(dropVertices){
			return output.dropIsolatedVertices();
		}else{
			return output;
		}

	}

	public GraphFrame filteredEdgesFromNeo4j(String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] conditions){
		return filteredEdgesFromNeo4j(url,user,password,nodesLabels,edgesProperties,conditions,true);
	}

	//FILTER EDGES AND NODES WHILE IMPORTING
	public GraphFrame importGraphFromNeo4j(String url, String user, String password, String[] nodesProperties, String[] edgesProperties,String[] nodesConditions,String[] edgesConditions,boolean dropVertices) {

		spark.sparkContext().conf().set("spark.neo4j.encryption.status","false")
				.set("spark.neo4j.url", url)//"bolt://localhost:7687"
				.set("spark.neo4j.user", user)//"neo4j"
				.set("spark.neo4j.password",password );//"Cirociro94"

		StructType schemaVertices=new StructType().add("id","String");
		StructType schemaEdges=new StructType().add("src","String").add("dst","String");

		String nodesString="";
		String edgesString="";
		String filter1="";
		String filter2="";

		for(int i=1;i<nodesProperties.length;i++){
			nodesString+=",toString(a."+nodesProperties[i]+")";
			schemaVertices=schemaVertices.add(nodesProperties[i],"String");
		}
		for(int i=1;i<nodesConditions.length;i++){
			filter1+="AND a."+nodesConditions[i];
			filter2+="AND a."+nodesConditions[i];
			filter2+="AND b."+nodesConditions[i];
		}
		for(int i=0;i<edgesProperties.length;i++){
			edgesString+=",toString(r."+edgesProperties[i]+")";
			schemaEdges=schemaEdges.add(edgesProperties[i],"String");
		}
		for(int i=0;i<edgesConditions.length;i++){
			filter2+="AND r."+edgesConditions[i];
		}

		Neo4j conn = new Neo4j(spark.sparkContext());


		RDD<Row> rddVertices=conn.cypher("MATCH (a) WHERE a."+nodesConditions[0]+" "+filter1+" RETURN toString(a."+nodesProperties[0]+")"+nodesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();
		RDD<Row> rddEdges=conn.cypher("MATCH (a)-[r]->(b) WHERE a."+nodesConditions[0]+" AND b."+nodesConditions[0]+" "+filter2+" RETURN toString(a."+nodesProperties[0]+"),toString(b."+nodesProperties[0]+")"+edgesString,JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadRowRdd();


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

	public GraphFrame importGraphFromNeo4j(String url, String user, String password, String[] nodesLabels, String[] edgesProperties,String[] nodesCondition,String[] edgesCondition){
		return importGraphFromNeo4j(url,user,password,nodesLabels,edgesProperties,nodesCondition,edgesCondition,true);
	}

	//public void toNeo4j(String url, String user, String password){
	//	spark.sparkContext().conf()
	//			.set("spark.neo4j.url", "bolt://localhost:7687")
	//			.set("spark.neo4j.user", "neo4j")
	//			.set("spark.neo4j.password", "Cirociro94");
	//	graphUtil.toNeo4J(spark);

	//}
	public void updateSubgraphLabels(String url, String user, String password,String reltype){
		Driver driver = GraphDatabase.driver(url, AuthTokens.basic(user, password));
		Session s =driver.session();
		String cql;

		for(Row r:graph.edges().toJavaRDD().collect()){
			cql="MATCH (a),(b) WHERE a.name='"+r.getString(0)+"' AND b.name='"+r.getString(1)+"' SET r."+reltype+"=true";
		}
		s.close();
	}

	// EXPLORATORY FUNCTIONS
	public Dataset<Row> vertices(){
		return graph.vertices();
	}
	public Dataset<Row> vertices(String condition){
		return graph.vertices().filter(condition);
	}
	public long verticesCounter(){
		return graph.vertices().count();
	}


	public Dataset<Row> edges(){
		return graph.edges();
	}
	public Dataset<Row> edges(String condition){
		return graph.edges().filter(condition);
	}
	public long edgesCounter(){
		return graph.edges().count();
	}

	public Dataset<Row> trianglesCounter(){
		return graph.triangleCount().run();
	}

	public double density(){
		double nVertices=graph.vertices().count();
		double nEdges=graph.edges().count();
		return  (2*nEdges)/(nVertices*(nVertices-1));
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


	//F4
	public GraphFrame xNeighborsGraph (ArrayList<Object> N, int x,boolean KEEP){

			Dataset<Row> paths=graph.shortestPaths().landmarks(N).run();
			Dataset<Row> explodedPaths=paths
					.select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")))
					.filter("value<="+x)
					.drop("value")
					.groupBy("id")
					.agg(org.apache.spark.sql.functions.collect_list("key").as("neighbors"));
			Dataset<Row> edges=graph.edges()
					.join(explodedPaths,graph.edges().col("src").equalTo(explodedPaths.col("id")));
			edges=edges.drop("id","neighbors");
			edges=edges.join(explodedPaths,edges.col("dst").equalTo(explodedPaths.col("id")));
			edges=edges.drop("id","neighbors");


			Dataset<Row> vertices=graph.vertices().join(explodedPaths,"id");

			if(KEEP){
				return GraphFrame.apply(vertices,edges);
			}else{
				return GraphFrame.fromEdges(edges);
			}
	}

	public GraphFrame xNeighborsGraph (ArrayList<Object> N, int x){
		return xNeighborsGraph(N,x,true);
	}
	//F1
	public Dataset<Row> findMaxComponent(ArrayList<Object> N, int x){
		if(x > 0){
			graph=xNeighborsGraph(N,x,false);
		}
		Dataset<Row> components=graph.connectedComponents().run();
		Tuple2<Long, Integer> max=components.javaRDD()
				.mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
				.mapToPair(new Ncounter2(N))
				.reduceByKey((i1,i2)->{return i1+i2;})
				.max(new comparator());
		Dataset<Row> maxComponent=components.filter("component="+max._1);

		return maxComponent;
	}


	public Dataset<Row> findMaxComponent(ArrayList<Object> N){
		return findMaxComponent(N,0);
	}

	//F2
	public Dataset<Row> componentsIntersection(ArrayList<Object> N,int x) throws IOException {

		if(x>0) {
			graph=xNeighborsGraph(N,x,false);
		}

		Dataset<Row> components=graph.connectedComponents().run();


		JavaRDD<Row> intersections=components.javaRDD()
				.mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
				.mapToPair(new Ncounter2(N))
				.reduceByKey((i1,i2)->{return i1+i2;})
				.map(t->{Row r=RowFactory.create(t._1.toString(),t._2);return r;});


		StructType schemaVertices=new StructType()
				.add("component-id","String")
				.add("|intersection|","Integer");

		Dataset<Row> output=spark.createDataFrame(intersections,schemaVertices);
		return output;
	}

	public Dataset<Row> componentsIntersection(ArrayList<Object> N) throws IOException {
		return componentsIntersection(N,0);
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

	//F5
	public Dataset<Row> xWeightedNeighbors(String inputNode, int x){

		Dataset<Row> edges=graph.edges().withColumn("weight", org.apache.spark.sql.functions.lit(-1));
		GraphFrame graph1=GraphFrame.fromEdges(edges);
		Dataset<Row> weightedPath= graphUtil.maxWeightedPaths(graph1,inputNode,spark);
		return weightedPath.filter("weight>="+x+" OR weight==0");
	}

	//F6
	public GraphFrame xNeighborsWeightedGraph(String inputNode, int x){
		Dataset<Row> xNeighbors=xWeightedNeighbors(inputNode,x);
		Dataset<Row> edges=graph.edges();
		edges=edges.join(xNeighbors,edges.col("src").equalTo(xNeighbors.col("id")));
		edges=edges.drop("id").drop("weight");
		edges=edges.join(xNeighbors,edges.col("dst").equalTo(xNeighbors.col("id")));
		edges=edges.drop("id").drop("weight");
		return GraphFrame.fromEdges(edges);
	}

	//F7
	public GraphFrame xNeighborsWeightedGraph(ArrayList<Object> input, int x){
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
		edges=edges.drop("id").drop("key");
		GraphFrame output=GraphFrame.apply(vertices,edges);
		return output;

	}

	//F8
	public GraphFrame neighborsWeightedGraph(ArrayList<ArrayList<Object>> input, int x){
		//List<String> data = Arrays.asList("A","B","C","D","E");
		SparkContext sparkContext=new SparkContext();
		Dataset<Row> edges=graph.edges().withColumn("weight", org.apache.spark.sql.functions.lit(-1));
		GraphFrame graph1=GraphFrame.fromEdges(edges);
		StructType s = new StructType()
				.add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("key", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("weight", DataTypes.DoubleType, true, Metadata.empty()));

		Dataset<Row> xNeighbors = spark.read().schema(s).csv(spark.emptyDataset(Encoders.STRING()));

		for(int i=0;i< input.size();i++){
			Dataset<Row> tmp=graphUtil.maxWeightedPaths(graph1, input.get(i),i,spark);
			xNeighbors=xNeighbors.union(tmp);
		}
		Dataset<Row> vertices=xNeighbors.filter("weight>="+x+" OR weight==0")
				.groupBy("id")
				.agg(org.apache.spark.sql.functions.collect_list("key").as("key"));
		edges=graph1.edges();
		edges=edges.join(vertices,edges.col("src").equalTo(vertices.col("id")));
		edges=edges.drop("id").drop("key");
		edges=edges.join(vertices,edges.col("dst").equalTo(vertices.col("id")));
		edges=edges.drop("id").drop("key");
		GraphFrame output=GraphFrame.apply(vertices,edges);
		return output;

	}

	//F9
	public GraphFrame weightedSubgraphWithLabels(ArrayList<ArrayList<Object>> input, int x){

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


		GraphFrame g=xNeighborsWeightedGraph(input.get(0),x);
		Dataset<Row> vertices=g.vertices();
		vertices=vertices.join(prova,vertices.col("id")
				.equalTo(prova.col("id1")),"left")
				.drop("id1");
		return GraphFrame.apply(vertices,g.edges());
	}



}
