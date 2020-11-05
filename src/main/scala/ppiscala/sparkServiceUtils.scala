package ppiscala

import java.util

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters


object sparkServiceUtils {

  def commonAncestors(g: Graph[java.lang.Long, java.lang.Long], c1:Long, c2:Long) : util.Set[java.lang.Long] = {
    var graph = g.mapVertices((_, goID) => (goID, (goID == c1, goID == c2)))

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
        if (a1 < a2 && areDisjointAncestors(g, c, a1, a2))
          disjAncestors_set.add(a1, a2)

    JavaConverters.mutableSetAsJavaSetConverter(disjAncestors_set).asJava
  }

  def areDisjointAncestors(g: Graph[java.lang.Long, java.lang.Long], c:Long, a1:Long, a2:Long) : Boolean = {
    // inizialmente c è 0, gli altri sono Inf
    var graph = g.mapVertices((_, goID) => (goID, if (goID == c) 0 else Integer.MAX_VALUE))

    graph = graph.pregel(Integer.MAX_VALUE, activeDirection=EdgeDirection.In)(

      // se il nuovo valore è Inf (non dovrebbe succedere) tengo il valore, altrimenti è 1 solo se entrambi sono 1
      (_, vertex, new_visited) => if (new_visited == Integer.MAX_VALUE) vertex
                                  else (vertex._1, vertex._2 * new_visited),

      // invio il messaggio alla srg se la dst non è Inf, inviando 1 solo se la dst è a1 o a2
      triplet => {
        if (triplet.dstAttr._2 != Integer.MAX_VALUE)
          Iterator((triplet.srcId, if (triplet.dstAttr._1 == a1 || triplet.dstAttr._1 == a2) 1 else 0))
        else
          Iterator.empty
      },

      // il nuovo valore è 1 solo se lo sono entrambi, se uno dei due è Inf aggiornare usando l'altro valore
      // non dovrebbero essere entrambi Inf, ma in caso ritornerebbe Inf
      (visited1, visited2) => if (visited1 == Integer.MAX_VALUE) visited2
                              else if (visited2 == Integer.MAX_VALUE) visited1
                              else visited1 * visited2
    )

    graph.vertices.filter(v_attr => (v_attr._2._1 == a1 || v_attr._2._1 == a2) && v_attr._2._2 == 1).count() == 2
  }

  def ancestors(g: Graph[java.lang.Long, java.lang.Long], c:Long) : util.Set[java.lang.Long] = {

    var graph = g.mapVertices((_, goID) => (goID, goID == c))

    graph = graph.pregel(false, activeDirection=EdgeDirection.In)(
      (_, vertex, new_visited) => (vertex._1, (vertex._2 || new_visited)),
      triplet => {

        if (!triplet.srcAttr._2 && triplet.dstAttr._2)
          Iterator((triplet.srcId, true))
        else
          Iterator.empty
      },
      (visited1, visited2) => (visited1 || visited2)
    )

    val set = graph.vertices.filter(v_attr => v_attr._2._2).mapValues(v_attr => v_attr._1).values.collect().toSet

    JavaConverters.setAsJavaSetConverter(set).asJava

  }

  def successors(g: Graph[java.lang.Long, java.lang.Long], c:Long) : util.Set[java.lang.Long] = {
    var graph = g.mapVertices((_, goID) => (goID, goID == c))

    graph = graph.pregel((false), activeDirection=EdgeDirection.Out)(
      (_, vertex, new_visited) => (vertex._1, vertex._2 || new_visited),
      triplet => {
        if (triplet.srcAttr._2 && !triplet.dstAttr._2)
          Iterator((triplet.dstId, true))
        else
          Iterator.empty
      },
      (visited1, visited2) => visited1 || visited2
    )

    val set = graph.vertices.filter(v_attr => v_attr._2._2).mapValues(v_attr => v_attr._1).values.collect().toSet

    JavaConverters.setAsJavaSetConverter(set).asJava
  }

}
