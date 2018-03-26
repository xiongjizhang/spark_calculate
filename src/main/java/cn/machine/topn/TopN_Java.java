package cn.machine.topn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * Created by zhao on 2018-02-24.
 */
public class TopN_Java {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: TopN <file>");
        }
        String inputPath = args[0];
        String outputPath = args[1];
        System.out.println("args[0] <file> = " + inputPath);

        Integer num = 5;
        String direction = "bottom"; // top bottom

        SparkConf conf = new SparkConf().setAppName("TopN");

        final JavaSparkContext ctx = new JavaSparkContext(conf);

        final Broadcast<Integer> broadcastN = ctx.broadcast(num);
        final Broadcast<String> broadcastString = ctx.broadcast(direction);

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        // <num>,<id>,<name>
        JavaPairRDD<String, Integer> pairs =
                lines.mapToPair(
                        new PairFunction<String, String, Integer>() {
                            public Tuple2<String, Integer> call(String s) throws Exception {
                                String[] tokens = s.split(",");

                                return new Tuple2<String, Integer>(tokens[1], new Integer(tokens[0]));
                            }
                        }
                );

        JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
                    public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                        final int N = broadcastN.value();
                        final String direction = broadcastString.value();

                        SortedMap<Integer, String> topn = new TreeMap<Integer, String>();
                        while (tuple2Iterator.hasNext()) {
                            Tuple2<String, Integer> tuple = tuple2Iterator.next();
                            topn.put(tuple._2(), tuple._1());
                            if (topn.size() > N) {
                                if (direction.toUpperCase().equals("TOP")) {
                                    topn.remove(topn.firstKey());
                                } else {
                                    topn.remove(topn.lastKey());
                                }
                            }
                        }
                        return Collections.singletonList(topn).iterator();
                    }
                }
        );

        SortedMap<Integer, String> finalTopn = new TreeMap<Integer, String>();

        List<SortedMap<Integer, String>> allTopn = partitions.collect();

        final int N = broadcastN.value();
        final String directionS = broadcastString.value();

        for (SortedMap<Integer, String> topn : allTopn){
            for (Map.Entry<Integer, String> tuple : topn.entrySet()){
                finalTopn.put(tuple.getKey(),tuple.getValue());
                if (finalTopn.size() > N) {
                    if (directionS.toUpperCase().equals("TOP")) {
                        finalTopn.remove(finalTopn.firstKey());
                    } else {
                        finalTopn.remove(finalTopn.lastKey());
                    }
                }
            }
        }

        SortedMap<Integer, String> finalTop10 = partitions.reduce(
                new Function2<SortedMap<Integer, String>, SortedMap<Integer, String>, SortedMap<Integer, String>>() {
                    public SortedMap<Integer, String> call(SortedMap<Integer, String> s1, SortedMap<Integer, String> s2) throws Exception {
                        final int N = broadcastN.value();
                        final String direction = broadcastString.value();

                        SortedMap<Integer, String> topn = new TreeMap<Integer, String>();
                        for (Map.Entry<Integer, String> tuple : s1.entrySet()){
                            topn.put(tuple.getKey(), tuple.getValue());
                            if (topn.size() > N) {
                                if (direction.toUpperCase().equals("TOP")) {
                                    topn.remove(topn.firstKey());
                                } else {
                                    topn.remove(topn.lastKey());
                                }
                            }
                        }

                        for (Map.Entry<Integer, String> tuple : s2.entrySet()){
                            topn.put(tuple.getKey(), tuple.getValue());
                            if (topn.size() > N) {
                                if (direction.toUpperCase().equals("TOP")) {
                                    topn.remove(topn.firstKey());
                                } else {
                                    topn.remove(topn.lastKey());
                                }
                            }
                        }

                        return topn;
                    }
                }
        );

        // sorted.saveAsTextFile(outputPath);
        System.out.println("========debug=========");
        for (Map.Entry<Integer, String> tuple : finalTopn.entrySet()) {
            System.out.println(tuple.getKey() + ", " + tuple.getValue());
        }
        System.out.println("===================");

        System.out.println("========debug2=========");
        for (Map.Entry<Integer, String> tuple : finalTop10.entrySet()) {
            System.out.println(tuple.getKey() + ", " + tuple.getValue());
        }
        System.out.println("===================");

    }
}
