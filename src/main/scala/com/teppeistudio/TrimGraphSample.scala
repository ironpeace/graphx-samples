package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object TrimGraphSample {

    def main(args : Array[String]) = {

  		val conf = new SparkConf()
  		val sc = new SparkContext("local", "test", conf)

      val graph = GraphLoader.edgeListFile(sc, "graphdata/trim_graph_sample.tsv").cache()

      println("\n\n Confirm Edges ~~~~~~~~~~~~~~~~~~~~~~")
      graph.edges.collect.foreach(println)

      val inTrimed = trim(graph, EdgeDirection.In)

      println("\n\n Confirm Vertices of inTrimed Graph ~~~~~~~~~~~~~~~~~~~~~~")
      inTrimed.vertices.collect.foreach(println)

      val trimed = trim(inTrimed, EdgeDirection.Out)

      println("\n\n Confirm Vertices of Trimed Graph ~~~~~~~~~~~~~~~~~~~~~~")
      trimed.vertices.collect.foreach(println)

  		sc.stop
    }

    def trim(graph: Graph[Int, Int], dir: EdgeDirection): Graph[Int, Int] = {

      val degrees: VertexRDD[Int] = dir match {
        case EdgeDirection.In => graph.outDegrees
        case EdgeDirection.Out => graph.inDegrees
      }

      Pregel(
        graph.outerJoinVertices(degrees)((id, attr, d) => (d.getOrElse(0), true)),
        initialMsg = Int.MaxValue,
        maxIterations = Int.MaxValue,
        activeDirection = dir
      )(
        (vid, attr, msg) => {
          if(msg == Int.MaxValue) attr
          else if(msg < attr._1) (msg, true)
          else (attr._1, false)
        },
        edge => {
          dir match {
            case EdgeDirection.In => {
              if(edge.dstAttr._2) Iterator((edge.srcId, edge.dstAttr._1))
              else Iterator.empty
            }
            case EdgeDirection.Out => {
              if(edge.srcAttr._2) Iterator((edge.dstId, edge.srcAttr._1))
              else Iterator.empty
            }
          }
        },
        (a, b) => a + b
      )
      .subgraph(vpred = (id, attr) => attr._1 > 0)
      .mapVertices((vid, attr) => 0)

    }

}
