package ppiscala

import java.util

import org.apache.spark.graphx._

import scala.collection.JavaConverters


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
    val disjAncestors_set = scala.collection.mutable.Set[(java.lang.Long, java.lang.Long)]()

    for (a1 <- ancestors_set)
      for (a2 <- ancestors_set)
        if (a1 < a2 && a1 != c && a2 != c && areDisjointAncestors(g, c, a1, a2))
          disjAncestors_set.add((a1, a2))

    JavaConverters.mutableSetAsJavaSetConverter(disjAncestors_set).asJava
  }

  def areDisjointAncestors(g: Graph[java.lang.Long, java.lang.Long], c:Long, a1:Long, a2:Long) : Boolean = {
    var graph = g.mapVertices((_, goId) => (goId, goId == c))

    graph = graph.pregel(false, activeDirection=EdgeDirection.In)(

      (_, vertex, new_visited) => (vertex._1, vertex._2 || new_visited),

      triplet => {
        if (triplet.dstAttr._1 != a1 && triplet.dstAttr._1 != a2)
          Iterator((triplet.srcId, true))
        else
          Iterator.empty
      },

      (visited1, visited2) => visited1 || visited2
    )

    graph.vertices.filter(v_attr => (v_attr._2._1 == a1 || v_attr._2._1 == a2) && v_attr._2._2).count() == 2
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
