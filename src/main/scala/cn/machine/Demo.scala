package cn.machine

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhao on 2017-12-17.
 */
object Demo {

  def main(args: Array[String]){
    var masterUrl = "local[1]"
    var inputPath = "F:\\bigdata_soft\\spark-2.2.0-bin-hadoop2.6\\README.md"
    var outputPath = "F:\\bigdata_soft\\output\\spark"

    if (args.length == 1) {
      masterUrl = args(0)
    } else if (args.length == 3) {
      masterUrl = args(0)
      inputPath = args(1)
      outputPath = args(2)
    }

    println(s"masterUrl:${masterUrl}, inputPath: ${inputPath}, outputPath: ${outputPath}")

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rowRdd = sc.textFile(inputPath)
    val resultRdd = rowRdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    resultRdd.saveAsTextFile(outputPath)
  }

}
