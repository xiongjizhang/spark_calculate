package cn.machine.secondarysort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by zhao on 2017-12-17.
 */
public class SecondarySort2 {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: secondarySort <file>");
        }
        String inputPath = args[0];
        String outputPath = args[1];
        System.out.println("args[0] <file> = " + inputPath);

        SparkConf conf = new SparkConf().setAppName("Secondary Sort");

        final JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD<String> lines = ctx.textFile(inputPath,1);

        // <name>,<time>,<value>
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs =
                lines.mapToPair(
                        new PairFunction<String, String, Tuple2<Integer, Integer>>() {
                            public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
                                String[] tokens = s.split(",");
                                // System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
                                Integer time = new Integer(tokens[1]);
                                Integer value = new Integer(tokens[2]);
                                Tuple2<Integer, Integer> timeValue = new Tuple2<Integer, Integer>(time, value);

                                return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0],timeValue);
                            }
                        }
                );

        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();

        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted =
                groups.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>() {
                    public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> tuple2s) throws Exception {
                        List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>((Collection<? extends Tuple2<Integer, Integer>>) tuple2s);
                        Collections.sort(newList, new Comparator<Tuple2<Integer, Integer>>() {
                            public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                                return o1._1().compareTo(o2._1());
                            }
                        });
                        return newList;
                    }
                });

        // sorted.saveAsTextFile(outputPath);

        System.out.println("========debug=========");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output = sorted.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output){
            Iterable<Tuple2<Integer, Integer>> list = t._2();
            System.out.println(t._1());
            for (Tuple2<Integer, Integer> tp : list){
                System.out.println(tp._1() + ", " + tp._2());
            }
            System.out.println("===================");
        }
    }
}
