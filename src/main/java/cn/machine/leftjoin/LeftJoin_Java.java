package cn.machine.leftjoin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by zhao on 2018-02-24.
 */
public class LeftJoin_Java {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Left Join <file>");
        }
        String inputPath = args[0];
        String outputPath = args[1];
        System.out.println("args[0] <file> = " + inputPath);

        SparkConf conf = new SparkConf().setAppName("Left Join");

        final JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD<String> users = ctx.textFile(inputPath+"\\user.txt", 1);
        JavaRDD<String> transactions = ctx.textFile(inputPath + "\\transaction.txt");

        // <num>,<id>,<name>
        JavaPairRDD<String, Tuple2<String, String>> usersRDD =
                users.mapToPair(
                        new PairFunction<String, String, Tuple2<String, String>>() {
                            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                                String[] tokens = s.split(",");
                                Tuple2<String, String> tuple = new Tuple2<String, String>("L",tokens[1]);
                                return new Tuple2<String, Tuple2<String, String>>(tokens[0], tuple);
                            }
                        }
                );

        JavaPairRDD<String, Tuple2<String, String>> transactionsRDD =
                transactions.mapToPair(
                        new PairFunction<String, String, Tuple2<String, String>>() {
                            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                                String[] tokens = s.split(",");
                                Tuple2<String, String> tuple = new Tuple2<String, String>("P", tokens[1]);
                                return new Tuple2<String, Tuple2<String, String>>(tokens[2], tuple);
                            }
                        }
                );

        JavaPairRDD<String, Tuple2<String, String>> allRDD = usersRDD.union(transactionsRDD);

        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD = allRDD.groupByKey();

        JavaPairRDD<String, String> productLocationsRDD = groupedRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, String>() {
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<Tuple2<String, String>>> stringIterableTuple2) throws Exception {
                Iterable<Tuple2<String, String>> pairs = stringIterableTuple2._2();
                String location = "UNKNOWN";
                List<String> products = new ArrayList<String>();
                for (Tuple2<String, String> tuple : pairs) {
                    if (tuple._1().equals("L")) {
                        location = tuple._2();
                    } else {
                        products.add(tuple._2());
                    }
                }
                List<Tuple2<String, String>> listlp = new ArrayList<Tuple2<String, String>>();
                for (String product : products) {
                    listlp.add(new Tuple2<String, String>(product, location));
                }
                return listlp.iterator();
            }
        });

        JavaPairRDD<String, Iterable<String>> productByLocations = productLocationsRDD.groupByKey();

        JavaPairRDD<String, Tuple2<Set<String>,Integer>> productByUniqueLocations = productByLocations.mapValues(
                new Function<Iterable<String>, Tuple2<Set<String>, Integer>>() {
                    public Tuple2<Set<String>, Integer> call(Iterable<String> strings) throws Exception {
                        Set<String> uniqueLocations = new HashSet<String>();
                        for (String location : strings) {
                            uniqueLocations.add(location);
                        }
                        return new Tuple2<Set<String>, Integer>(uniqueLocations,uniqueLocations.size());
                    }
                }
        );

        // sorted.saveAsTextFile(outputPath);

        List<Tuple2<String, Tuple2<Set<String>, Integer>>> result = productByUniqueLocations.collect();

        System.out.println("========debug=========");
        for (Tuple2<String, Tuple2<Set<String>, Integer>> tuple : result) {
            System.out.println("===> " + tuple._1() + ": " + tuple._2()._2());
            for (String location : tuple._2()._1()){
                System.out.println("     " + tuple._1() + ": " + location);
            }
        }
        System.out.println("===================");

    }
}
