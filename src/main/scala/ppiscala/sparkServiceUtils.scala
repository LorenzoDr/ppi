package ppiscala

import java.util

import org.apache.spark.graphx._

import scala.collection.{JavaConverters, mutable}


object sparkServiceUtils {

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
      disjointAncestors_set ++= _disjointAncestors(g, c, a1, a2_set.toBuffer)
    }

    JavaConverters.mutableSetAsJavaSetConverter(disjointAncestors_set).asJava
  }

  def disjointAncestors(g: Graph[java.lang.Long, java.lang.Long], c1: Long, ancestors1_set: util.Set[java.lang.Long], c2: Long, ancestors2_set: util.Set[java.lang.Long], batch_size: Int) : util.Set[(java.lang.Long, java.lang.Long)] = {
    val ancestors1 = JavaConverters.asScalaSetConverter(ancestors1_set).asScala.toArray
    val ancestors2 = JavaConverters.asScalaSetConverter(ancestors2_set).asScala.toArray

    val ancestors1_pairs = mutable.Buffer[(java.lang.Long, java.lang.Long)]()
    val ancestors2_pairs = mutable.Buffer[(java.lang.Long, java.lang.Long)]()

    val disjointAncestors_set = mutable.Set[(java.lang.Long, java.lang.Long)]()

    for (idx_a11 <- 0 until ancestors1.length-1 if ancestors1(idx_a11) != c1)
      for (idx_a21 <- idx_a11+1 until ancestors1.length if ancestors1(idx_a21) != c1)
        ancestors1_pairs.append((ancestors1(idx_a11), ancestors1(idx_a21)))

    for (idx_a12 <- 0 until ancestors2.length-1 if ancestors2(idx_a12) != c2)
      for (idx_a22 <- idx_a12+1 until ancestors2.length if ancestors2(idx_a22) != c2)
        ancestors2_pairs.append((ancestors2(idx_a12), ancestors2(idx_a22)))

    var i = 0
    val min_len = Math.min(ancestors1_pairs.length, ancestors2_pairs.length)

    while (i < min_len) {
      disjointAncestors_set ++= _disjointAncestors(g, c1, ancestors1_pairs.slice(i, Math.min(i+batch_size, min_len)), c2, ancestors2_pairs.slice(i, Math.min(i+batch_size, min_len)))
      i += batch_size
    }

    i = min_len

    if (min_len < ancestors1_pairs.length)
      while (i < ancestors1_pairs.length) {
        disjointAncestors_set ++= _disjointAncestors(g, c1, ancestors1_pairs.slice(i, Math.min(i+batch_size, ancestors1_pairs.length)))
        i += batch_size
      }

    else if (min_len < ancestors2_pairs.length)
      while (i < ancestors2_pairs.length) {
        disjointAncestors_set ++= _disjointAncestors(g, c2, ancestors2_pairs.slice(i, Math.min(i+batch_size, ancestors2_pairs.length)))
        i += batch_size
      }

    JavaConverters.mutableSetAsJavaSetConverter(disjointAncestors_set).asJava
  }

  private def _disjointAncestors(g: Graph[java.lang.Long, java.lang.Long], c:Long, a1:Long, a2_array: mutable.Buffer[java.lang.Long]) : mutable.Set[(java.lang.Long, java.lang.Long)] = {
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
      if (graph.vertices.filter(v_attr => (v_attr._2._1 == a1 || v_attr._2._1 == a2_array(i)) && v_attr._2._2(i)).count() == 2) {
        pair_set.add((a1, a2_array(i)))
        pair_set.add((a2_array(i), a1))
      }

    pair_set
  }

  private def _disjointAncestors(g: Graph[java.lang.Long, java.lang.Long], c:Long, a_pairs: mutable.Buffer[(java.lang.Long, java.lang.Long)]) : mutable.Set[(java.lang.Long, java.lang.Long)] = {
    var graph = g.mapVertices((_, goId) => (goId, Array.fill(a_pairs.length)(goId == c)))

    graph = graph.pregel(Array.fill(a_pairs.length)(false), activeDirection=EdgeDirection.In)(

      (_, vertex, new_value) => (vertex._1, vertex._2.zip(new_value).map(value_pair => value_pair._1 || value_pair._2)),

      triplet => {
        val new_value = Array.fill(a_pairs.length)(false)
        var to_update = false

        for (i <- a_pairs.indices)
          if (!triplet.srcAttr._2(i) && triplet.dstAttr._2(i) && triplet.dstAttr._1 != a_pairs(i)._1 && triplet.dstAttr._1 != a_pairs(i)._2) {
            new_value(i) = true
            to_update = true
          }

        if (to_update)
          Iterator((triplet.srcId, new_value))
        else
          Iterator.empty
      },

      (value1, value2) => value1.zip(value2).map(value_pair => value_pair._1 || value_pair._2)
    )

    val pair_set = mutable.Set[(java.lang.Long, java.lang.Long)]()

    for (i <- a_pairs.indices)
      if (graph.vertices.filter(v_attr => (v_attr._2._1 == a_pairs(i)._1 || v_attr._2._1 == a_pairs(i)._2) && v_attr._2._2(i)).count() == 2) {
        pair_set.add(a_pairs(i))
        pair_set.add((a_pairs(i)._2, a_pairs(i)._1))
      }

    pair_set
  }

  private def _disjointAncestors(g: Graph[java.lang.Long, java.lang.Long], c1: Long, a_pairs1: mutable.Buffer[(java.lang.Long, java.lang.Long)], c2: Long, a_pairs2: mutable.Buffer[(java.lang.Long, java.lang.Long)]) : mutable.Set[(java.lang.Long, java.lang.Long)] = {
    var graph = g.mapVertices((_, goId) => (goId, (Array.fill(a_pairs1.length)(goId == c1), Array.fill(a_pairs2.length)(goId == c2))))

    graph = graph.pregel((Array.fill(a_pairs1.length)(false), Array.fill(a_pairs2.length)(false)), activeDirection=EdgeDirection.In)(

      (_, vertex, new_value) => (vertex._1, (vertex._2._1.zip(new_value._1).map(value_pair => value_pair._1 || value_pair._2), vertex._2._2.zip(new_value._2).map(value_pair => value_pair._1 || value_pair._2))),

      triplet => {
        val new_value = (Array.fill(a_pairs1.length)(false), Array.fill(a_pairs2.length)(false))
        var to_update = false

        for (i <- a_pairs1.indices)
          if (!triplet.srcAttr._2._1(i) && triplet.dstAttr._2._1(i) && triplet.dstAttr._1 != a_pairs1(i)._1 && triplet.dstAttr._1 != a_pairs1(i)._2) {
            new_value._1(i) = true
            to_update = true
          }

        for (i <- a_pairs2.indices)
          if (!triplet.srcAttr._2._2(i) && triplet.dstAttr._2._2(i) && triplet.dstAttr._1 != a_pairs2(i)._1 && triplet.dstAttr._1 != a_pairs2(i)._2) {
            new_value._2(i) = true
            to_update = true
          }

        if (to_update)
          Iterator((triplet.srcId, new_value))
        else
          Iterator.empty
      },

      (value1, value2) => (value1._1.zip(value2._1).map(value_pair => value_pair._1 || value_pair._2), value1._2.zip(value2._2).map(value_pair => value_pair._1 || value_pair._2))
    )

    val pair_set = mutable.Set[(java.lang.Long, java.lang.Long)]()

    for (i <- a_pairs1.indices)
      if (graph.vertices.filter(v_attr => (v_attr._2._1 == a_pairs1(i)._1 || v_attr._2._1 == a_pairs1(i)._2) && v_attr._2._2._1(i)).count() == 2) {
        pair_set.add(a_pairs1(i))
        pair_set.add((a_pairs1(i)._2, a_pairs1(i)._1))
      }

    for (i <- a_pairs2.indices)
      if (graph.vertices.filter(v_attr => (v_attr._2._1 == a_pairs2(i)._1 || v_attr._2._1 == a_pairs2(i)._2) && v_attr._2._2._2(i)).count() == 2) {
        pair_set.add(a_pairs2(i))
        pair_set.add((a_pairs2(i)._2, a_pairs2(i)._1))
      }

    pair_set
  }
}
