package cn.machine.association_rules

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.JavaConverters._

/**
 * Created by zhao on 2018-03-05.
 */
object FindAssociationRules {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: Find Association Rules <file>")
    }
    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)
    println("args[0] <file> = " + inputPath)

    val conf = new SparkConf().setAppName("Find Association Rules")

    val ctx = new SparkContext(conf)

    val transactions : RDD[String] = ctx.textFile(inputPath)

    val patterns = transactions.flatMap( transaction => {
      val items = transaction.trim.split(",").toList
      // val rules1 = Combination.findSortedCombinations(items.asJavaCollection).asScala
      val rules = (1 to items.size).map{i:Int => items.combinations(i).toList}.flatten
      rules.filter(rule => rule.length > 0).map( rule => {
        (rule,1)
      }).toList
    })

    val combined = patterns.reduceByKey{ case (a,b) => a+b}

    val subPatterns = combined.flatMap{case(list,frequency) => {
      if (list.size == 1) {
        List((list.toList, (List(), frequency)))
      } else {
        val list1 = (0 until list.size).map( i => {
          val clone = new util.ArrayList[String](list.toList.asJava)
          clone.remove(i)
          (clone.asScala.toList, (list.toList, frequency))
        }).toList
        List((list.toList, (List(), frequency))) ::: list1
      }
      // return List
    }}

    val assocRules = subPatterns.groupByKey().map{ case (fromlist, toLists) => {
      val list = toLists.filter( item => !item._1.isEmpty)
      val fromCount = toLists.find(item => item._1.isEmpty).get._2.toDouble
      list.map{case (toList, frequency) => {
        (fromlist, toList.filter(item => !fromlist.contains(item)), frequency/fromCount)
      }}.toList
    }}

    assocRules.collect().foreach(list => {
      list.foreach(tuple3 => {
        print(tuple3._1 + "->" + tuple3._2 + ":" + tuple3._3 + ",")
      })
      println()
    })

  }

}
