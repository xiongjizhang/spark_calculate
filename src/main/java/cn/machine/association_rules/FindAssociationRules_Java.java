package cn.machine.association_rules;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zhao on 2018-03-02.
 */
public class FindAssociationRules_Java {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: FindAssociationRules <file>");
        }
        String inputPath = args[0];
        String outputPath = args[1];
        System.out.println("args[0] <file> = " + inputPath);

        SparkConf conf = new SparkConf().setAppName("Left Join");

        final JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD<String> transactions = ctx.textFile(inputPath);

        JavaPairRDD<List<String>, Integer> patterns = transactions.flatMapToPair(new PairFlatMapFunction<String, List<String>, Integer>() {
            public Iterator<Tuple2<List<String>, Integer>> call(String s) throws Exception {
                List<String> items = toList(s);
                List<Tuple2<List<String>, Integer>> result = new ArrayList<Tuple2<List<String>, Integer>>();
                for (List<String> comb : Combination.findSortedCombinations(items)) {
                    if (comb.size() > 0) {
                        result.add(new Tuple2<List<String>, Integer>(comb, 1));
                    }
                }
                return result.iterator();
            }
        });

        JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subPatterns =
                combined.flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>, Integer>, List<String>, Tuple2<List<String>, Integer>>() {
                    public Iterator<Tuple2<List<String>, Tuple2<List<String>, Integer>>> call(Tuple2<List<String>, Integer> listIntegerTuple2) throws Exception {
                        List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<Tuple2<List<String>, Tuple2<List<String>, Integer>>>();
                        List<String> list = listIntegerTuple2._1();
                        Integer frequency = listIntegerTuple2._2();
                        result.add(new Tuple2<List<String>, Tuple2<List<String>, Integer>>(list, new Tuple2<List<String>, Integer>(null, frequency)));
                        if (list.size() == 1) {
                            return result.iterator();
                        }
                        for (int i = 0; i < list.size(); i++) {
                            List<String> subList = removeOneItem(list, i);
                            result.add(new Tuple2<List<String>, Tuple2<List<String>, Integer>>(subList, new Tuple2<List<String>, Integer>(list, frequency)));
                        }
                        return result.iterator();
                    }
                });

        JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subPatterns.groupByKey();

        JavaRDD<List<Tuple3<List<String>, List<String>, Double>>> assocRules =
                rules.map(new Function<Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>>, List<Tuple3<List<String>, List<String>, Double>>>() {
                    public List<Tuple3<List<String>, List<String>, Double>> call(Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>> listIterableTuple2) throws Exception {
                        List<String> fromList = listIterableTuple2._1();
                        Iterable<Tuple2<List<String>, Integer>> to = listIterableTuple2._2();
                        List<Tuple2<List<String>, Integer>> toList = new ArrayList<Tuple2<List<String>, Integer>>();
                        Tuple2<List<String>, Integer> fromCount = null;
                        for (Tuple2<List<String>, Integer> item : to) {
                            if (item._1() == null) {
                                fromCount = item;
                            } else {
                                toList.add(item);
                            }
                        }

                        List<Tuple3<List<String>, List<String>, Double>> result = new ArrayList<Tuple3<List<String>, List<String>, Double>>();
                        if (toList.isEmpty()) {
                            return result;
                        }
                        for (Tuple2<List<String>, Integer> item : toList) {
                            Tuple3<List<String>, List<String>, Double> tuple3 = new Tuple3<List<String>, List<String>, Double>(fromList, item._1(), ((double)item._2())/fromCount._2());
                            result.add(tuple3);
                        }
                        return result;
                    }
                });

        List<List<Tuple3<List<String>, List<String>, Double>>> result = assocRules.collect();

        System.out.println("========debug=========");
        for (List<Tuple3<List<String>, List<String>, Double>> resultRules : result) {
            for (Tuple3<List<String>, List<String>, Double> tuple3 : resultRules) {
                List<String> toList = tuple3._2();
                toList.removeAll(tuple3._1());
                System.out.print(tuple3._1() + "-> " + toList + ":" + tuple3._3() + ",");
            }
            System.out.println();
        }
        System.out.println("===================");

        System.exit(0);
    }

    static List<String> removeOneItem(List<String> list, int i) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        if (i < 0 || i > (list.size()-1)) {
            return list;
        }
        List<String> clone = new ArrayList<String>(list);
        clone.remove(i);
        return clone;
    }

    static List<String> toList(String line){
        List<String> items = new ArrayList<String>();
        if (line == null || line.trim().length() == 0) {
            return null;
        }
        String[] tokens = line.split(",");
        for (String item : tokens) {
            items.add(item);
        }
        return items;
    }

}
