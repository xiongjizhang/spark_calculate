package cn.machine.common_friends;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zhao on 2018-03-05.
 */
public class FindCommonFriends_Java {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Find Common Friends <file>");
        }
        String inputPath = args[0];
        String outputPath = args[1];
        System.out.println("args[0] <file> = " + inputPath);

        SparkConf conf = new SparkConf().setAppName("Find Common Friends");

        final JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD<String> sourceData = ctx.textFile(inputPath);

        JavaPairRDD<String, String> friends =
                sourceData.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
                    public Iterator<Tuple2<String, String>> call(String s) throws Exception {
                        if (s == null || s.trim().length() == 0) {
                            return null;
                        }

                        String[] tokens = s.split(",");
                        String user = tokens[0];
                        String[] friends = tokens[1].trim().split(" ");
                        List<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
                        for (String friend : friends) {
                            if (user.compareTo(friend) < 0) {
                                result.add(new Tuple2<String, String>(user + ", " + friend, tokens[1].trim()));
                            } else {
                                result.add(new Tuple2<String, String>(friend + ", " + user, tokens[1].trim()));
                            }
                        }
                        return result.iterator();
                    }
                });

        JavaPairRDD<String, String> commonFriends = friends.reduceByKey(new Function2<String, String, String>() {
            public String call(String s, String s2) throws Exception {
                StringBuffer result = new StringBuffer();
                for (String f1 : s.split(" ")) {
                    int flag = 0;
                    for (String f2 : s2.split(" ")) {
                        if (f1.equals(f2)) {
                            flag = 1;
                            break;
                        }
                    }
                    if (flag == 1) {
                        result.append(f1 + " ");
                    }
                }
                return result.toString().trim();
            }
        });

        List<Tuple2<String, String>> result = commonFriends.collect();

        System.out.println("========debug=========");
        for (Tuple2<String, String> tuple2 : result) {
            System.out.println(tuple2._1() + " : " + tuple2._2());
        }
        System.out.println("===================");

        System.exit(0);

    }
}
