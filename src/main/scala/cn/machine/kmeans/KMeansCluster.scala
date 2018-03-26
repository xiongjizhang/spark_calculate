package cn.machine.kmeans

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhao on 2018-03-09.
 */
object KMeansCluster {

  def main(args: Array[String]) {
    if (args.length < 5) {
      println("Usage: KMeans <file>")
    }
    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)
    val centerNum = args.apply(2).toInt
    val numIterations =args.apply(3).toInt
    val numRuns = args.apply(4).toInt

    println("args[0] <file> = " + inputPath)

    val conf = new SparkConf().setAppName("KMeans")

    val ctx = new SparkContext(conf)

    val sourceData: RDD[String] = ctx.textFile(inputPath)

    val point: RDD[Vector] = sourceData.map(line => {
      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    }).cache()

    val kMeansModel = KMeans.train(point,centerNum, numIterations, numRuns, KMeans.K_MEANS_PARALLEL)

    kMeansModel.clusterCenters.foreach(println(_))

  }

}
