
package org.benhurdelhey

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.util.GraphGenerators

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import scala.util.Random

case class Triplet (srcGroup : Long , dstGroup : Long , weight : Double , rank : Long, originalSrc : Long, originalDst : Long)

object RandomizedMinCut {

  private var tripletRankOrdering : Ordering[Triplet] = new Ordering[Triplet]() {
      override def compare(x: Triplet, y: Triplet): Int = 
          Ordering[Long].compare(x.rank, y.rank)
    }
  
  private def contractEdge(edges: RDD[Triplet]): RDD[Triplet] = {
    val max_rank = edges.max()(tripletRankOrdering)
    val oldGroup = math.min(max_rank.srcGroup , max_rank.dstGroup)
    val newGroup = math.max(max_rank.srcGroup , max_rank.dstGroup) 
    edges
      .map(e => 
        if (e.dstGroup == oldGroup) 
          e.copy(dstGroup=newGroup)
        else if (e.srcGroup == oldGroup) 
          e.copy(srcGroup=newGroup)
        else e)
      .filter(e => (e.srcGroup != e.dstGroup))
  }

  private def slowMincut(triplets: RDD[Triplet], numVertices : Long): (Double, RDD[Triplet]) = {
    var edges = triplets
    var numComponents = numVertices // first stage contractEdgeions
    while (numComponents > 2) {
      //println("components " + components)
      //println(edges.collect().mkString("\n"))
      edges = contractEdge(edges) 
      numComponents = numComponents - 1
      if (( numComponents % 200) == 0) {
        edges.checkpoint()
      }
    }

    // sum the weight for edges which are a candidate for the mincut
    (edges.map(e => e.weight).sum, edges)
  }

  private def randomEdgeRank(): Long = { //
    val rand = new Random()
    rand.nextLong()
  }

  private def fastMincut(orig_edges : RDD[Triplet], numVertices : Long) : (Double, RDD[Triplet]) = {
    if (numVertices <= 6) {
      slowMincut(orig_edges , numVertices)
    } else {
      val t = math.ceil(1 + numVertices / math.sqrt(2)).toInt 
      var edges1 : RDD[Triplet] = orig_edges.map(e => e.copy(rank=randomEdgeRank()) )
      var edges2 : RDD[Triplet] = orig_edges.map(e => e.copy(rank=randomEdgeRank()) )
      edges1.cache
      edges2.cache
      var components = numVertices
       
      while (components > t) {
        edges1 = contractEdge(edges1) 
        edges2 = contractEdge(edges2) 
        components = components - 1
        if (( components % 200) == 0) {
          edges1.checkpoint
          edges2.checkpoint
        } 
      }
      val res1 = fastMincut(edges1, components)
      val res2 = fastMincut(edges2, components)
      if (res1._1 < res2._1)
        res1
      else
        res2
    }
  }

  def run(triplets : RDD[Triplet], numVertices : Long, trials : Long): (Double, RDD[Triplet], RDD[Triplet]) = {
    var min_cut: (Double, RDD[Triplet]) = (Double.PositiveInfinity, null)
    val begin : Long = 1
    triplets.cache()

    begin to trials foreach { i =>
      println("iteration " + i)
      val cut = fastMincut(triplets, numVertices)
      min_cut = if (cut._1 < min_cut._1) cut else min_cut}
    
    val new_triplets = triplets
      .subtract(min_cut._2
        .map(triplet => triplet
          .copy(rank=0, srcGroup=triplet.originalSrc, dstGroup=triplet.originalDst)))

    (min_cut._1, min_cut._2, new_triplets)
  }

}

object PersonDeduplicator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.memory.fraction", "0.8")
      .getOrCreate

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir(args(0))
    
    val numVertices : Int = 40
    val numEdges: Int = 400
    val g : Graph[Long, Triplet] = GraphGenerators.logNormalGraph(sc, numVertices=numVertices, numEParts=32)
      .mapVertices((id, _) => id)
      .mapTriplets(e => Triplet(e.srcId, e.dstId, 2.0, 0, e.srcId, e.dstId)) 
      .subgraph(epred = (triplet) => (triplet.srcId != triplet.dstId))
      .partitionBy(PartitionStrategy.EdgePartition2D, 6)

    var triplets = g.edges.map(e => e.attr)
    //println(triplets.collect().mkString("\n"))

    val numTrials: Long = math.pow(math.log(g.numVertices.toDouble), 2).toLong
    println("NUM TRIALS: " + numTrials)

    val result = RandomizedMinCut.run(triplets, g.numVertices, numTrials)
    
    println("MIN CUT VALUE: " + result._1)
    println("MIN CUT EDGES \n" + result._2.collect().mkString("\n"))
    //println("NEW TRIPLETS \n" + result._3.collect().mkString("\n"))

    spark.stop()
  }

}

