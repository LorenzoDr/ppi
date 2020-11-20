package ppiscala

import java.io.File

import org.graphframes.GraphFrame
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame,SaveMode, Dataset, Row, SparkSession}
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.graphx.lib.PageRank
import org.neo4j.spark._
import sun.security.util.Password



object graphUtil {

  def nodesFromNeo4j(spark:SparkSession,url:String,user:String,password:String,propRef:String,condition:String): DataFrame ={

    var wCondition=""
    var properties=""

    if(!condition.equals("")){
      wCondition="WHERE p."+condition.split(",")(0)
      for(i<-1 to condition.split(",").length-1){
        wCondition+=" AND p."+condition.split(",")(i)+" "
      }
    }

    if(!propRef.equals("")){
      val propList=propRef.split(",")
      for(i<-0 to propList.length-1){
        properties+=",p."+propList(i)
      }
    }

    val query="MATCH (p:protein) "+wCondition+" RETURN a.name"+properties


    val output=spark.read.format("org.neo4j.spark.DataSource")
      .option("url",url)
      .option("authentication.basic.username",user)
      .option("authentication.basic.password",password)
      .option("query",query)
      .load()

    return output
  }
  def nodesFromNeo4j(spark:SparkSession,url:String,user:String,password:String,propRef:String)={
    nodesFromNeo4j(spark,url,user,password,propRef,"")
  }
  def edgesFromNeo4j(spark:SparkSession,url:String,user:String,password:String,propRef:String,condition:String): DataFrame ={
    var wCondition=""
    var properties=""

    if(!condition.equals("")){
      wCondition="WHERE "+condition.split(",")(0)
      for(i<-1 to condition.split(",").length-1){
        wCondition+=" AND "+condition.split(",")(i)+" "
      }
    }

    if(!propRef.equals("")){
      val propList=propRef.split(",")
      for(i<-0 to propList.length-1){
        properties+=",r."+propList(i)
      }
    }
    val query="MATCH (a:protein)-[r]->(b:protein) RETURN a.name AS src,b.name AS dst"

    val output=spark.read.format("org.neo4j.spark.DataSource")
      .option("url",url)
      .option("authentication.basic.username",user)
      .option("authentication.basic.password",password)
      .option("query",query)
      .load()

    return output

  }
  def edgesFromNeo4j(spark:SparkSession,url:String,user:String,password:String,propRef:String)={
    edgesFromNeo4j(spark,url,user,password,propRef,"")
  }
  def filteredEdgesFromNeo4j(spark:SparkSession,url:String,user:String,password:String,condition:String)={
    edgesFromNeo4j(spark,url,user,password,"",condition)
  }
  def edgesFromNeo4j(spark:SparkSession,url:String,user:String,password:String)={
      edgesFromNeo4j(spark,url,user,password,"","")
  }


  //EXPORT TO NEO4J

  def graphToNeo4J(df:DataFrame,url:String,user:String,password: String,rel:String)={
    val propertiesArray=df.drop("src").drop("dst").columns
    var properties=propertiesArray(0);
    for(i<-1 to propertiesArray.length-1){
      properties+=","+propertiesArray(i)
    }
    df.write
      .format("org.neo4j.spark.DataSource")
      .option("url", url)
      .option("authentication.basic.username",user)
      .option("authentication.basic.password",password)
      .option("relationship", "INTERACTION")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":protein")
      .option("relationship.source.save.mode", "overwrite")
      .option("relationship.source.node.keys", "src:name")
      .option("relationship.target.labels", ":protein")
      .option("relationship.target.node.keys", "dst:name")
      .option("relationship.target.save.mode", "overwrite")
      .option("relationship.properties", properties)
      .save()
  }

 def updateVertices(df:DataFrame,url:String,user:String,password: String,attr:String,referenceCol:String,properties:String)={
   val propertiesList=properties.split(",")
   var newProperties="p."+propertiesList(0)+"=event."+propertiesList(0)

   if(propertiesList.length>1){
     for(i<-1 to propertiesList.length-1){
       newProperties+=","+"p."+propertiesList(i)+"=event."+propertiesList(i)
     }
   }

   df.write
     .format("org.neo4j.spark.DataSource")
     .option("url", url)
     .option("authentication.basic.username",user)
     .option("authentication.basic.password",password)
     .option("query", "MATCH (p:protein {"+attr+": event."+referenceCol+"}) SET "+newProperties)
     .save()
 }

  // LONGEST PATH

  def dijkstra(g: GraphFrame, sourceNode:String, weightIndex:Int,spark: SparkSession)= {

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
