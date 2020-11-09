package ppiscala

import java.util

import org.apache.spark.graphx._

import scala.collection.{JavaConverters, mutable}


object sparkServiceUtils {

  def commonAncestors(g: Graph[java.lang.Long, java.lang.Long], c1:Long, c2:Long) : util.Set[java.lang.Long] = {
    var graph = g.mapVertices((_, goId) => (goId, (goId == c1, goId == c2)))

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

    val set = graph.vertices.filter(v_attr => v_attr._2._2._1 && v_attr._2._2._2).mapValues(v_attr => v_attr._1).values.collect().toSet

    JavaConverters.setAsJavaSetConverter(set).asJava
  }

  def disjointAncestors(g: Graph[java.lang.Long, java.lang.Long], c: Long, ancestors: util.Set[java.lang.Long]) : util.Set[(java.lang.Long, java.lang.Long)] = {
    val ancestors_set = JavaConverters.asScalaSetConverter(ancestors).asScala
    val disjointAncestors_set = mutable.Set[(java.lang.Long, java.lang.Long)]()

    for (a1 <- ancestors_set if a1 != c) {
      val a2_set = ancestors_set.filter(a2 => a1 < a2 && a2 != c)
      disjointAncestors_set ++= _disjointAncestors(g, c, a1, a2_set.toArray)
    }

    JavaConverters.mutableSetAsJavaSetConverter(disjointAncestors_set).asJava
  }

  private def _disjointAncestors(g: Graph[java.lang.Long, java.lang.Long], c:Long, a1:Long, a2_array:Array[java.lang.Long]) : mutable.Set[(java.lang.Long, java.lang.Long)] = {
    var graph = g.mapVertices((_, goId) => (goId, Array.fill(a2_array.length)(goId == c)))

    graph = graph.pregel(Array.fill(a2_array.length)(false), activeDirection=EdgeDirection.In)(

      (_, vertex, new_value) => (vertex._1, vertex._2.zip(new_value).map(value_pair => value_pair._1 || value_pair._2)),

      triplet => {
        val new_value = Array.fill(a2_array.length)(false)
        var to_update = false

        if (triplet.dstAttr._1 == a1)
          Iterator.empty
        else {
          for (i <- a2_array.indices)
            if (!triplet.srcAttr._2(i) && triplet.dstAttr._2(i) && triplet.dstAttr._1 != a2_array(i)) {
              new_value(i) = true
              to_update = true
            }
            else
              new_value(i) = false

          if (to_update)
            Iterator((triplet.srcId, new_value))
          else
            Iterator.empty
        }
      },

      (value1, value2) => value1.zip(value2).map(value_pair => value_pair._1 || value_pair._2)
    )

    val pair_set = mutable.Set[(java.lang.Long, java.lang.Long)]()

    for (i <- a2_array.indices)
      if (graph.vertices.filter(v_attr => (v_attr._2._1 == a1 || v_attr._2._1 == a2_array(i)) && v_attr._2._2(i)).count() == 2)
        pair_set.add(a1, a2_array(i))

    pair_set
  }

  def ancestors(g: Graph[java.lang.Long, java.lang.Long], c:Long) : util.Set[java.lang.Long] = {

    var graph = g.mapVertices((_, goId) => (goId, goId == c))

    graph = graph.pregel(false, activeDirection=EdgeDirection.In)(

      (_, vertex, new_visited) => (vertex._1, vertex._2 || new_visited),

      triplet => {

        if (!triplet.srcAttr._2 && triplet.dstAttr._2)
          Iterator((triplet.srcId, true))
        else
          Iterator.empty
      },

      (visited1, visited2) => visited1 || visited2
    )

    visitedSet(graph)
  }

  def successors(g: Graph[java.lang.Long, java.lang.Long], c:Long) : util.Set[java.lang.Long] = {
    var graph = g.mapVertices((_, goId) => (goId, goId== c))

    graph = graph.pregel(false, activeDirection=EdgeDirection.Out)(

      (_, vertex, new_visited) => (vertex._1, vertex._2 || new_visited),

      triplet => {
        if (triplet.srcAttr._2 && !triplet.dstAttr._2)
          Iterator((triplet.dstId, true))
        else
          Iterator.empty
      },

      (visited1, visited2) => visited1 || visited2
    )

    visitedSet(graph)
  }

  private def visitedSet(graph: Graph[(java.lang.Long, Boolean), java.lang.Long]): util.Set[java.lang.Long] = {
    JavaConverters.setAsJavaSetConverter(graph.vertices.filter(v_attr => v_attr._2._2).mapValues(v_attr => v_attr._1).values.collect().toSet[java.lang.Long]).asJava
  }
}
