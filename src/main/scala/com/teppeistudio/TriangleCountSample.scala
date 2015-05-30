package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

object TriangleCountSample {

    def main(args : Array[String]) = {

  		val conf = new SparkConf()
  		val sc = new SparkContext("local", "test", conf)

      //val graph = GraphLoader.edgeListFile(sc, "graphdata/triangle_sample_01.tsv").cache()*/
      val graph = GraphLoader.edgeListFile(sc, "graphdata/triangle_sample_02.tsv").cache()

      println("\n\n Confirm Edges ~~~~~~~~~~~~~~~~~~~~~~")
      graph.edges.collect.foreach(println)

      val triangled = graph
                  .triangleCount
                  .subgraph(vpred = (id, attr) => attr > 0)
                  .connectedComponents
                  .vertices
                  .map(v => (v._2, v._1))
                  .sortByKey(true)

      println("\n\n Confirm Triangled ~~~~~~~~~~~~~~~~~~~~~~")
      triangled.collect.foreach(println)

  		sc.stop
    }
}
