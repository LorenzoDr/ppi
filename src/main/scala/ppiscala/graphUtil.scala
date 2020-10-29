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
     // format("com.databricks.spark.csv").
    df
      .coalesce(1)
      .write
      .mode ("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .save("filename.csv")

  }


  def toNeo4J(g: GraphFrame,spark: SparkSession) = {

    Neo4jGraph.saveGraph(spark.sparkContext, g.toGraphX, nodeProp = "id", null, merge = true)

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

  def maxWeightedPaths(g: GraphFrame, s: String, spark: SparkSession,weightIndex :Int): DataFrame = {
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
          ctx.sendToDst(ctx.srcAttr._2 + ctx.attr.getInt(weightIndex)),//42
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
