package cn.machine.leftjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhao on 2018-02-10.
  */
object LeftJoin {
   def main(args: Array[String]) {
     if (args.length < 1) {
       println("Usage: Left Join <file>")
     }
     val inputPath: String = args.apply(0)
     val outputPath: String = args.apply(1)
     println("args[0] <file> = " + inputPath)

     val conf = new SparkConf().setAppName("Left Join")

     val ctx = new SparkContext(conf)

     val users = ctx.textFile(inputPath + "\\user.txt")
     val transactions = ctx.textFile(inputPath + "\\transaction.txt")

     val usersRDD = users.map(user => {
       val tokens = user.split(",")
       (tokens.apply(0),("L",tokens.apply(1)))
     })

     val transactionsRDD = transactions.map(transaction => {
       val tokens = transaction.split(",")
       (tokens.apply(2),("P",tokens.apply(1)))
     })

     val allRDD = usersRDD.union(transactionsRDD)

     val groupedRDD = allRDD.groupByKey()

    val productLocations = groupedRDD.flatMap{case (userid,iter) => {
      var location = "UNKNOWN"
      var products = List[String]()
      for (tuple <- iter) {
        if (tuple._1.equals("L")) {
          location = tuple._2
        } else {
          products = products ::: List(tuple._2)
        }
      }
      var plList = List[(String,String)]()

      for (product <- products) {
        plList = plList ::: List((product,location))
      }
      plList
    }}

     val productBylocations = productLocations.groupByKey()

     val productByUniqueLocations = productBylocations.map{ case (product, locations) => {
       val arrayBuf = ArrayBuffer[String]()
       for (location <- locations) {
         if (!arrayBuf.contains(location)) arrayBuf += location
       }
       (product,arrayBuf.toIterable)
     }}

     val result = productByUniqueLocations.collect()
     println("=======debug========")
     result.foreach{ case (key, list) =>{
       println("===> " + key + ": " + list.toList.size)
       list.foreach(tp => println("===> " + key + ", " + tp))
       println("=================")
     }}
   }
 }
