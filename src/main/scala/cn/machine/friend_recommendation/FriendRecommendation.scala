package cn.machine.friend_recommendation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhao on 2018-03-06.
 */
object FriendRecommendation {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: Find Association Rules <file>")
    }
    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)
    println("args[0] <file> = " + inputPath)

    val conf = new SparkConf().setAppName("Find Association Rules")

    val ctx = new SparkContext(conf)

    val sourceData : RDD[String] = ctx.textFile(inputPath)

    val commonFriends = sourceData.flatMap(line => {
      val tokens = line.split(",")
      val user = tokens.apply(0)
      val friends = tokens.apply(1).trim.split(" ")
      val list1 = friends.map(friend => {(user, (friend, "-1"))}).toList
      val list2 = friends.toList.combinations(2).map(list => {(list.apply(0), (list.apply(1), user))}).toList
      val list3 = friends.toList.combinations(2).map(list => {(list.apply(1), (list.apply(0), user))}).toList
      list1 ::: list2 ::: list3
    })

    val recommendations = commonFriends.groupByKey().mapValues(iter => {
      if (iter.filter{case (f,m) => "-1".equals(m)}.size > 0) {
        ""
      } else {

      }
      iter.groupBy{case(f,m) => f}.filter{case(key,iter) => {iter.filter{case (f,m) => "-1".equals(m)}.size == 0}}.map{
        case (key, iter) => {
          val mutualFriends = iter.map(_._2).toList.distinct
          key + " (" + mutualFriends.size + ": [" + mutualFriends.mkString(",") + "]),"
        }
      }.toList.mkString
    })

    recommendations.foreach{case (key, value) => {println(key + "  " + value)}}

  }
}
