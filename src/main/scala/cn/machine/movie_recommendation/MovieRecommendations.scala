package cn.machine.movie_recommendation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhao on 2018-03-07.
 */
object MovieRecommendations {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: Movie Recommendations <file>")
    }
    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)
    println("args[0] <file> = " + inputPath)

    val conf = new SparkConf().setAppName("Movie Recommendations")

    val ctx = new SparkContext(conf)

    val sourceData: RDD[String] = ctx.textFile(inputPath)

    val movieRating = sourceData.map( line => {
      val tokens = line.split(",")
      (tokens.apply(1), (tokens.apply(0), tokens.apply(2)))
    })

    val userRating = movieRating.groupByKey().flatMap{ case (movie,iter) => {
      val list = iter.toList
      val count = list.length
      list.map{ case (user, rating) => {(user, (movie, rating, count))}}
    }}

    val movies = userRating.groupByKey().flatMap{ case (user, iter) => {
      iter.toList.combinations(2).map(list => {
        val data1 = list.apply(0)
        val data2 = list.apply(1)
        if (data1._1.compareTo(data2._1) < 0) {
          ((data1._1, data2._1),(Integer.parseInt(data1._2), data1._3, Integer.parseInt(data2._2), data2._3))
        } else {
          ((data2._1, data1._1),(Integer.parseInt(data2._2), data2._3, Integer.parseInt(data1._2), data1._3))
        }
      })
    }}

    val movieCorr = movies.groupByKey().mapValues(iter => {
      val list = iter.toList;
      val dotProduct = list.map{ case (r1,c1,r2,c2) => r1*r2}.sum.toDouble
      val rating1Sum = list.map{ case (r1,c1,r2,c2) => r1}.sum.toDouble
      val rating2Sum = list.map{ case (r1,c1,r2,c2) => r2}.sum.toDouble
      val rating1Norm = list.map{ case (r1,c1,r2,c2) => r1*r1}.sum.toDouble
      val rating2Norm = list.map{ case (r1,c1,r2,c2) => r2*r2}.sum.toDouble
      val size = list.length
      val maxCount1 = list.map{ case (r1,c1,r2,c2) => c1}.max
      val maxCount2 = list.map{ case (r1,c1,r2,c2) => c2}.max

      val pearsonCor = calPearson(size,dotProduct, rating1Sum, rating2Sum, rating1Norm, rating2Norm)
      val consineCor = calConsine(dotProduct, rating1Norm, rating2Norm)
      val jaccardCor = calJaccard(size, maxCount1, maxCount2)

      (pearsonCor, consineCor, jaccardCor)
    })

    movieCorr.collect().foreach{case((m1,m2),(p,c,j)) => println(m1+","+m2+":"+p+" "+c+" "+j)}

  }

   def calPearson(size: Int, dotProduct: Double, rating1Sum: Double, rating2Sum: Double, rating1Norm: Double, rating2Norm: Double): Double = {
    return (size * dotProduct - rating1Sum * rating2Sum) / Math.sqrt(size * rating1Norm - rating1Sum * rating1Sum) / Math.sqrt(size * rating2Norm - rating2Sum * rating2Sum)
  }

   def calConsine(dotProduct: Double, rating1Norm: Double, rating2Norm: Double): Double = {
    return dotProduct / Math.sqrt(rating1Norm) / Math.sqrt(rating2Norm)
  }

   def calJaccard(size: Int, maxCount1: Int, maxCount2: Int): Double = {
    return (size.toDouble) / (maxCount1 + maxCount2 - size)
  }

}
