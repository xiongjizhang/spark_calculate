package cn.machine.topn

import java.util
import java.util.Collections

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.JavaConversions._

/**
 * Created by zhao on 2018-02-26.
 */
object TopN {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: TopN <file>")
    }
    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)
    println("args[0] <file> = " + inputPath)

    val conf = new SparkConf().setAppName("TopN")

    val ctx = new SparkContext(conf)

    val num = 5
    val direction = "top" // top bottom

    val broadcastN = ctx.broadcast(num)
    val broadcastString = ctx.broadcast(direction)

    val lines = ctx.textFile(inputPath)

    val pairs: RDD[(String, Int)] = lines.map( line => {
      val tokens = line.split(",")
      (tokens.apply(1), tokens.apply(0).toInt)
    })

    val partitions = pairs.mapPartitions{ iter => {
      val N = broadcastN.value
      val direction = broadcastString.value

      val topn = new util.TreeMap[Int, String]()
      while (iter.hasNext) {
        val tuple = iter.next()
        topn.put(tuple._2, tuple._1)
        if (topn.size() > N) {
          if (direction.toUpperCase().equals("TOP")) {
            topn.remove(topn.firstKey());
          } else {
            topn.remove(topn.lastKey());
          }
        }
      }
      Collections.singletonList(topn).iterator()
    }}

    val finalTopN = partitions.reduce{ case (t1, t2) => {
      val N = broadcastN.value
      val direction = broadcastString.value

      val topn = new util.TreeMap[Int, String]()
      t1.entrySet()
      for ( tuple <- t1.entrySet()) {
        topn.put(tuple.getKey, tuple.getValue)
        if (topn.size() > N) {
          if (direction.toUpperCase().equals("TOP")) {
            topn.remove(topn.firstKey());
          } else {
            topn.remove(topn.lastKey());
          }
        }
      }

      for ( tuple <- t2.entrySet()) {
        topn.put(tuple.getKey, tuple.getValue)
        if (topn.size() > N) {
          if (direction.toUpperCase().equals("TOP")) {
            topn.remove(topn.firstKey());
          } else {
            topn.remove(topn.lastKey());
          }
        }
      }
      topn}}

    finalTopN.toList.foreach{case (value,key) => println(value + ", " + key)}

  }
}
