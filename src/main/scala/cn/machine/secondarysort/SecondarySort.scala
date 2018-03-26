package cn.machine.secondarysort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhao on 2018-02-10.
 */
object SecondarySort {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: secondarySort <file>")
    }
    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)
    println("args[0] <file> = " + inputPath)

    val conf = new SparkConf().setAppName("Secondary Sort")

    val ctx = new SparkContext(conf)

    val lines = ctx.textFile(inputPath)

    val pairs: RDD[(String, (Int, Int))] = lines.map(line => {
      val tokens: Array[String] = line.split(",")
      (tokens.apply(0),(tokens.apply(1).toInt, tokens.apply(2).toInt))
    })

    val groups: RDD[(String, Iterable[(Int, Int)])] = pairs.groupByKey();

    val sorted: RDD[(String, Iterable[(Int, Int)])] = groups.map{ case (key, list) => {
      (key,list.toList.sortBy{case(tp1,tp2) => tp1}.toIterable)
    }}

    println("=======debug========")
    sorted.foreach{ case (key, list) =>{
      println(key)
      list.foreach(tp => println(tp._1 + ", " + tp._2))
      println("=================")
    }}
  }
}
