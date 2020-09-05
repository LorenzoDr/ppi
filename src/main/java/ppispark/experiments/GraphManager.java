package experiments;

import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;
import org.neo4j.spark.Neo4j;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

public class GraphManager {
	
	//LOAD GRAPH FROM TSV FILE
	public GraphFrame fromTsv(SparkSession spark, String path) {
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
	        graph.vertices().show();
	        graph.edges().show();
	        return graph;
    }
	
	//EXPORT GRAPH TO TSV TEXT FILE
	public void toTsv() {}
	
	//LOAD GRAPH FROM NEO4J
	public GraphFrame fromNeo4j(SparkSession spark) {
		Neo4j conn = new Neo4j(spark.sparkContext());
		Dataset<Row> d = conn.cypher("MATCH (a)-[r]->(b) RETURN a.name AS src,b.name AS dst,r.alt_id_A AS alt_id_A,r.alt_id_B AS alt_id_B,r.alias_A AS alias_A,r.alias_B AS alias_B,r.det_method AS det_method,r.first_auth AS first_auth,r.id_pub AS id_pub,r.ncbi_id_A AS ncbi_id_A,r.ncbi_id_B AS ncbi_id_B,r.int_types AS int_types,r.source_db AS source_db,r.int_id AS int_id,r.conf_score AS conf_score,r.comp_exp AS comp_exp,r.bio_role_A AS bio_role_A,r.bio_role_B AS bio_role_B,r.exp_role_A AS exp_role_A,r.exp_role_B AS exp_role_B,r.type_A AS type_A,r.type_B AS type_B,r.xref_A AS xref_A,r.xref_B AS xref_B,r.xref_int AS xref_int,r.annot_A AS annot_A,r.annot_B AS annot_B,r.annot_int AS annot_int,r.ncbi_id_organism AS ncbi_id_organism,r.param_int AS param_int,r.create_data AS create_data,r.up_date AS up_date,r.chk_A AS chk_A,r.chk_B AS chk_B,r.chk_int AS chk_int,r.negative AS negative,r.feat_A AS feat_A,r.feat_B AS feat_B,r.stoich_A AS stoich_A,r.stoich_B AS stoich_B,r.part_meth_A AS part_meth_A,r.part_meth_B AS part_meth_B",JavaConverters.mapAsScalaMapConverter(new HashMap<String,Object>()).asScala().toMap( Predef.<Tuple2<String, Object>>conforms())).loadDataFrame();
		GraphFrame graph = GraphFrame.fromEdges(d);
		return graph;
	}
	
	//EXPORT GRAPH TO NEO4J
	public void toNeo4j() {
		/*
		 * Row[] result = (Row[]) ppi_network.edges().collect(); for (Row row : result)
		 * { System.out.println(row.get(41));}
		 */
	}

	
	
	//CREATE A GRAPHFRAME FROM TWO ARRAYLISTS: VERTICES AND EDGES
	public GraphFrame fromLists (SparkSession spark,List<User> vertices,List<Relationship> edges) {
			
			Dataset<Row> userDataset = spark.createDataFrame(vertices, User.class);//.withColumn("new_col",functions.lit(1));
	        Dataset<Row> relationshipDataset = spark.createDataFrame(edges, Relationship.class);
	        GraphFrame graph = new GraphFrame(userDataset, relationshipDataset);
		    graph.vertices().show();
		    graph.edges().show();
			return graph;
		
   }

}
