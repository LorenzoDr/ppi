package ppiscala

import java.io.File

import org.graphframes.GraphFrame
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.graphx.lib.PageRank
import org.neo4j.spark._
import org.neo4j.spark.cypher.{NameProp, Pattern}



object graphUtil {

  def main(args: Array[String]): Unit = {
    /*
     a (1)   b (2)
       \   /
         c (3)  i (9)
       /   \   /
     d (4)   e (5)
       \   /
         f (6)
       /   \
     g (7)   h (8)

     */

    val spark = SparkSession.builder().appName("test-goscore").master("local[*]").getOrCreate()

    val nodes : RDD[(VertexId, java.lang.Long)] = spark.sparkContext.parallelize(Seq((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)))
    val edges : RDD[Edge[java.lang.Long]] = spark.sparkContext.parallelize(Seq(Edge(1, 3, 1), Edge(2, 3, 1), Edge(3, 4, 1), Edge(3, 5, 1), Edge(4, 6, 1), Edge(5, 6, 1), Edge(6, 7, 1), Edge(6, 8, 1), Edge(9, 5, 1)))
    val graph = Graph(nodes, edges)

//    val graphFrame = GraphFrame.fromGraphX(graph)

    println(commonAncestors(graph, 4, 5).mkString("Array(", ", ", ")"))
  }

  def commonAncestors(g: Graph[java.lang.Long, java.lang.Long], id1:Long, id2:Long) : Set[java.lang.Long] = {
    var graph = g.mapVertices((_, goID) => (goID, (goID == id1, goID == id2)))

    graph = graph.pregel((false, false), activeDirection=EdgeDirection.In)(
      (_, vertex, new_visited) => (vertex._1, (vertex._2._1 || new_visited._1, vertex._2._2 || new_visited._2)),
      triplet => {
        val to_update = (!triplet.srcAttr._2._1 && triplet.dstAttr._2._1) || (!triplet.srcAttr._2._2 && triplet.dstAttr._2._2)

        if (to_update)
          Iterator((triplet.srcId, (triplet.srcAttr._2._1 || triplet.dstAttr._2._1, triplet.srcAttr._2._2 || triplet.dstAttr._2._2)))
        else
          Iterator.empty
      },
      (visited1, visited2) => (visited1._1 || visited2._1, visited1._2 || visited2._2)
    )

    graph.vertices.filter(v_attr => v_attr._2._2._1 && v_attr._2._2._2).mapValues(v_attr => v_attr._1).values.collect().toSet
  }

  /* def dijkstra(g: GraphFrame, sourceNode:String, weightIndex:Int,spark: SparkSession)= {

     val initialGraph = g.toGraphX.mapVertices((_, attr) => {
       val vertex = (attr.getString(0), if (attr.getString(0) == sourceNode) 1.0 else 0.0)
       vertex
     })

     val distanceGraph = initialGraph.pregel(0.0)(
       (_, attr, newDist) => (attr._1, math.max(attr._2, newDist)),
       triplet => {
         //Distance accumulator
         if (triplet.srcAttr._2 + triplet.attr.getInt(weightIndex) > triplet.dstAttr._2) {
           Iterator((triplet.dstId, triplet.srcAttr._2 + triplet.attr.getInt(weightIndex)))
         } else {
           Iterator.empty
         }
       },
       (a, b) => math.max(a, b)
     )

     distanceGraph.vertices.foreach(println)
   }*/




/*  def toNeo4J(g: GraphFrame,spark: SparkSession) = {

    g.toGraphX.
    //Neo4jGraph.saveGraph(spark.sparkContext, g.toGraphX, nodeProp = null, null, merge = true)

  }*/

/*

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
 */

}
