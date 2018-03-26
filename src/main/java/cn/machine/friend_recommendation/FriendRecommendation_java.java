package cn.machine.friend_recommendation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by zhao on 2018-03-06.
 */
public class FriendRecommendation_java {

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

        JavaPairRDD<String, Tuple2<String, String>> commonFriends =
                sourceData.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, String>>() {
                    public Iterator<Tuple2<String, Tuple2<String, String>>> call(String s) throws Exception {
                        if (s == null || s.trim().length() == 0) {
                            return null;
                        }

                        String[] tokens = s.trim().split(",");
                        String user = tokens[0];
                        String[] friends = tokens[1].trim().split(" ");

                        List<Tuple2<String, Tuple2<String, String>>> result = new ArrayList<Tuple2<String, Tuple2<String, String>>>();
                        for (String friend : friends) {
                            result.add(new Tuple2<String, Tuple2<String, String>>(user, new Tuple2<String, String>(friend, "-1")));
                        }

                        for (int i = 0; i < friends.length; i++) {
                            for (int j = i + 1; j < friends.length; j++) {
                                result.add(new Tuple2<String, Tuple2<String, String>>(friends[i], new Tuple2<String, String>(friends[j], user)));
                                result.add(new Tuple2<String, Tuple2<String, String>>(friends[j], new Tuple2<String, String>(friends[i], user)));
                            }
                        }

                        return result.iterator();
                    }
                });

        JavaPairRDD<String, String> recommondations =
                commonFriends.groupByKey().mapValues(new Function<Iterable<Tuple2<String, String>>, String>() {
                    public String call(Iterable<Tuple2<String, String>> tuple2s) throws Exception {
                        Map<String, List<String>> mutualFriends = new HashMap<String, List<String>>();
                        for (Tuple2<String, String> tuple : tuple2s) {
                            boolean isFriend = tuple._2().equals("-1");
                            if (isFriend) {
                                mutualFriends.put(tuple._1(), null);
                            } else {
                                if (mutualFriends.containsKey(tuple._1())) {
                                    if (mutualFriends.get(tuple._1()) != null && !mutualFriends.get(tuple._1()).contains(tuple._2())) {
                                        mutualFriends.get(tuple._1()).add(tuple._2());
                                    }
                                } else {
                                    List<String> newList = new ArrayList<String>();
                                    newList.add(tuple._2());
                                    mutualFriends.put(tuple._1(), newList);
                                }
                            }
                        }
                        return buildOutput(mutualFriends);
                    }
                });

        List<Tuple2<String, String>> result = recommondations.collect();

        for (Tuple2<String, String> tuple : result) {
            System.out.println(tuple._1() + "  " + tuple._2());
        }

    }

    public static String buildOutput(Map<String, List<String>> mutualFriends) {
        StringBuffer buffer = new StringBuffer();
        for (Map.Entry<String, List<String>> mutualFriend : mutualFriends.entrySet()) {
            String key = mutualFriend.getKey();
            List<String> values = mutualFriend.getValue();
            if (values == null)
                continue;
            buffer.append(key);
            buffer.append(" (" + values.size() + ": " + values.toString() + "), ");
        }
        return buffer.toString();
    }

}
