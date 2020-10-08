package ppiscala

import java.io.File

import org.graphframes.GraphFrame
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import java.util

import org.apache.spark.graphx.lib.PageRank
import org.neo4j.spark._
import org.neo4j.spark.cypher.{NameProp, Pattern}



object graphUtil {

  def saveDfToCsv(df: DataFrame, tsvOutput: String) {
    val tmpParquetDir = "Posts.tmp.parquet"

    df.repartition(1).write.
      format("com.databricks.spark.csv").
      option("header", "true").
      option("delimiter", "\t").
      save(tmpParquetDir)

    val dir = new File(tmpParquetDir)
    val newFileRgex = tmpParquetDir
    val tmpTsfFile = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString
    (new File(tmpTsfFile)).renameTo(new File(tsvOutput))

    dir.listFiles.foreach( f => f.delete )
    dir.delete
  }
  def toNeo4J(spark: SparkSession) = {
    val neo=Neo4j(spark.sparkContext)
    //val graphQuery = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as source, id(m) as target, type(r) as value SKIP $_skip LIMIT $_limit"
    //val gra: Graph[Long, String] = neo.rels(graphQuery).partitions(7).batch(200).loadGraph

    //println(gra.vertices.count)
    //    => 100
    // print(gra.edges.count)
    //    => 1000

    // load graph via pattern
    // note ("Person","id") refers to Person.id and ("Person",null) refers to id(Person) in cypher
    val graph = neo.pattern(("Person","id"),("KNOWS","since"),("Person","id")).loadGraph[Long,Long]


   //println(graph.vertices.count)
    //    => 100
   // println(graph.edges.count)
    //    => 1000
    //val graph2 = PageRank.run(graph, 5)
    //    => graph2: org.apache.spark.graphx.Graph[Double,Double] =

    //    => res46: Array[(org.apache.spark.graphx.VertexId, Long)]
    //    => Array((236746,100), (236745,99), (236744,98))

    // uses pattern from above to save the data, merge parameter is false by default, only update existing nodes
    //neo.saveGraph(graph, "rank")
    // uses pattern from parameter to save the data, merge = true also create new nodes and relationships
    neo.saveGraph(graph, "rank",Pattern(NameProp("Person","id"),Seq[NameProp](NameProp("FRIEND","years")),NameProp("Person","id")), merge = true)
  }





  def dfSchema(columnNames: List[String]): StructType =
    StructType(
      Seq(
        StructField(name = "id", dataType = StringType, nullable = false),
        StructField(name = "weight", dataType = DoubleType, nullable = false)
      )
    )

  def dfSchema2(columnNames: List[String]): StructType =
    StructType(
      Seq(
        StructField(name = "id", dataType = StringType, nullable = false),
        StructField(name = "key", dataType = StringType, nullable = false),
        StructField(name = "weight", dataType = DoubleType, nullable = false)
      )
    )

  def maxWeightedPaths(g: GraphFrame, s: String, spark: SparkSession): DataFrame = {
    var i = 0
    val verticesRDD = g.toGraphX.vertices
    val origin = verticesRDD.filter(t => t._2.getString(0).equals(s)).collect()(0)._1
    var g1 = g.toGraphX.mapVertices(
      (vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue))

    for (i <- 1L to g.vertices.count) {
      val currentVertexId =
        g1.vertices.filter(!_._2._1)
          .fold((0L, (false, Double.MaxValue)))((a, b) =>
            if (a._2._2 < b._2._2) a else b)
          ._1
      val newDistances = g1.aggregateMessages[Double](
        ctx => if (ctx.srcId == currentVertexId)
          ctx.sendToDst(ctx.srcAttr._2 + ctx.attr.getInt(42)),
        (a, b) => math.max(a, b))

      g1 = g1.outerJoinVertices(newDistances)((vid, vd, newSum) =>
        (vd._1 || vid == currentVertexId,
          math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
    }


    val j = g1.vertices.join(verticesRDD).map(t => Row(t._2._2.get(0), -1 * t._2._1._2)) //.collect().foreach(println)

    val schema = dfSchema(List("id", "weight"))
    val output = spark.createDataFrame(j, schema)

    return output
  }

  def maxWeightedPaths(g: GraphFrame, landmarks: util.ArrayList[Any], spark: SparkSession): Dataset[Row] = {
    var i = 0
    val graph = g.toGraphX
    var outputRDD = spark.sparkContext.emptyRDD[Row]

    for (l <- landmarks.toArray) {
      val verticesRDD = graph.vertices
      val origin = verticesRDD.filter(t => t._2.getString(0).equals(l)).collect()(0)._1
      var g1 = g.toGraphX.mapVertices(
        (vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue))

      for (i <- 1L to g.vertices.count) {
        val currentVertex = g1.vertices.filter(!_._2._1)
          .fold((0L, (false, Double.MaxValue)))((a, b) =>
            if (a._2._2 < b._2._2) a else b)
        val currentVertexId = currentVertex._1
        val newDistances = g1.aggregateMessages[Double](
          ctx => if (ctx.srcId == currentVertexId)
            ctx.sendToDst(ctx.srcAttr._2 + ctx.attr.getInt(42)),
          (a, b) => math.max(a, b))

        g1 = g1.outerJoinVertices(newDistances)((vid, vd, newSum) =>
          (vd._1 || vid == currentVertexId,
            math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
      }
      val j = g1.vertices.join(verticesRDD).map(t => Row(t._2._2.get(0), l, -1 * t._2._1._2))

      outputRDD = outputRDD.union(j)
    }
    val schema = dfSchema2(List("id", "key", "weight"))
    val output = spark.createDataFrame(outputRDD, schema)
    //return output.filter(t => t.getDouble(2) != Double.MinValue)
    return output
  }



  def maxWeightedPaths(g: GraphFrame, landmarks: util.ArrayList[Any],i:Int, spark: SparkSession): Dataset[Row] = {
    val graph = g.toGraphX
    var outputRDD = spark.sparkContext.emptyRDD[Row]

    for (l <- landmarks.toArray) {
      val verticesRDD = graph.vertices
      val origin = verticesRDD.filter(t => t._2.getString(0).equals(l)).collect()(0)._1
      var g1 = g.toGraphX.mapVertices(
        (vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue))

      for (i <- 1L to g.vertices.count) {
        val currentVertex = g1.vertices.filter(!_._2._1)
          .fold((0L, (false, Double.MaxValue)))((a, b) =>
            if (a._2._2 < b._2._2) a else b)
        val currentVertexId = currentVertex._1
        val newDistances = g1.aggregateMessages[Double](
          ctx => if (ctx.srcId == currentVertexId)
            ctx.sendToDst(ctx.srcAttr._2 + ctx.attr.getInt(42)),
          (a, b) => math.max(a, b))

        g1 = g1.outerJoinVertices(newDistances)((vid, vd, newSum) =>
          (vd._1 || vid == currentVertexId,
            math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
      }
      val j = g1.vertices.join(verticesRDD).map(t => Row(t._2._2.get(0), i.toString, -1 * t._2._1._2))

      outputRDD = outputRDD.union(j)
    }
    val schema = dfSchema2(List("id", "key", "weight"))
    val output = spark.createDataFrame(outputRDD, schema)
    return output
  }




}
