package cn.machine.movie_recommendation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zhao on 2018-03-07.
 */
public class MovieRecommendations_Java {

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

        JavaPairRDD<String, Tuple2<String,String>> movieRating =
                sourceData.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
                    public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                        if (s == null || s.length() == 0) {
                            return null;
                        }
                        String[] tokens = s.split(",");
                        return new Tuple2<String, Tuple2<String, String>>(tokens[1], new Tuple2<String, String>(tokens[0], tokens[2]));
                    }
                });

        JavaPairRDD<String, Tuple3<String, String, String>> userRating =
                movieRating.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, Tuple3<String, String, String>>() {
                    public Iterator<Tuple2<String, Tuple3<String, String, String>>> call(Tuple2<String, Iterable<Tuple2<String, String>>> stringIterableTuple2) throws Exception {
                        String movie = stringIterableTuple2._1();
                        Iterable<Tuple2<String, String>> iter = stringIterableTuple2._2();
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                        int size = 0;
                        for (Tuple2<String, String> tuple : iter) {
                            size++;
                            list.add(tuple);
                        }
                        List<Tuple2<String, Tuple3<String, String, String>>> result = new ArrayList<Tuple2<String, Tuple3<String, String, String>>>();
                        for (Tuple2<String, String> tuple : list) {
                            result.add(new Tuple2<String, Tuple3<String, String, String>>(tuple._1(), new Tuple3<String, String, String>(movie, tuple._2(), "" + size)));
                        }
                        return result.iterator();
                    }
                });

        JavaPairRDD<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Integer>> movies =
                userRating.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple3<String, String, String>>>, Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Integer>>() {
                    public Iterator<Tuple2<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Integer>>> call(Tuple2<String, Iterable<Tuple3<String, String, String>>> stringIterableTuple2) throws Exception {
                        List<Tuple3<String, String, String>> list = new ArrayList<Tuple3<String, String, String>>();
                        for (Tuple3<String, String, String> tuple3 : stringIterableTuple2._2()) {
                            list.add(tuple3);
                        }
                        List<Tuple2<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Integer>>> result =
                                new ArrayList<Tuple2<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Integer>>>();
                        for (int i = 0; i < list.size(); i++) {
                            for (int j = i+1; j < list.size(); j++) {
                                Tuple3<String, String, String> tuple1 = list.get(i);
                                Tuple3<String, String, String> tuple2 = list.get(j);
                                String movie1 = tuple1._1();
                                Integer rating1 = Integer.parseInt(tuple1._2());
                                Integer ratingCount1 = Integer.parseInt(tuple1._3());
                                String movie2 = tuple2._1();
                                Integer rating2 = Integer.parseInt(tuple2._2());
                                Integer ratingCount2 = Integer.parseInt(tuple2._3());

                                if (movie1.compareTo(movie2) < 0) {
                                    Tuple2<String, String> movieTuple = new Tuple2<String, String>(movie1, movie2);
                                    Tuple4<Integer, Integer, Integer, Integer> value = new Tuple4<Integer, Integer, Integer, Integer>(rating1,ratingCount1,rating2,ratingCount2);
                                    result.add(new Tuple2<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Integer>>(movieTuple,value));
                                } else {
                                    Tuple2<String, String> movieTuple = new Tuple2<String, String>(movie2, movie1);
                                    Tuple4<Integer, Integer, Integer, Integer> value = new Tuple4<Integer, Integer, Integer, Integer>(rating2,ratingCount2,rating1,ratingCount1);
                                    result.add(new Tuple2<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Integer>>(movieTuple,value));
                                }
                            }
                        }
                        return result.iterator();
                    }
                });

        JavaPairRDD<Tuple2<String, String>, Tuple3<Double, Double, Double>> movieCorr =
                movies.groupByKey().mapValues(new Function<Iterable<Tuple4<Integer, Integer, Integer, Integer>>, Tuple3<Double, Double, Double>>() {
                    public Tuple3<Double, Double, Double> call(Iterable<Tuple4<Integer, Integer, Integer, Integer>> tuple4s) throws Exception {
                        double dotProduct = 0;
                        double rating1Sum = 0;
                        double rating2Sum = 0;
                        double rating1Norm = 0;
                        double rating2Norm = 0;
                        int size = 0;
                        int maxCount1 = 0;
                        int maxCount2 = 0;
                        for (Tuple4<Integer, Integer, Integer, Integer> tuple : tuple4s) {
                            dotProduct += tuple._1().doubleValue() * tuple._3().doubleValue();
                            rating1Sum += tuple._1().doubleValue();
                            rating2Sum += tuple._3().doubleValue();
                            rating1Norm += tuple._1().doubleValue() * tuple._1().doubleValue();
                            rating2Norm += tuple._3().doubleValue() * tuple._3().doubleValue();
                            size++;
                            if (maxCount1 < tuple._2()) {
                                maxCount1 = tuple._2();
                            }
                            if (maxCount2 < tuple._4()) {
                                maxCount2 = tuple._4();
                            }
                        }

                        double pearsonCor = calPearson(size, dotProduct, rating1Sum, rating2Sum, rating1Norm, rating2Norm);

                        double consineCor = calConsine(dotProduct, rating1Norm, rating2Norm);

                        double jaccardCor = calJaccard(size, maxCount1, maxCount2);
                        return new Tuple3<Double, Double, Double>(pearsonCor, consineCor, jaccardCor);
                    }
                });

        List<Tuple2<Tuple2<String, String>, Tuple3<Double, Double, Double>>> result = movieCorr.collect();

        for (Tuple2<Tuple2<String, String>, Tuple3<Double, Double, Double>> tuple : result) {
            System.out.println(tuple._1()._1() + "," + tuple._1()._2() + ": " + tuple._2()._1() + " " + tuple._2()._2() + " " + tuple._2()._3());
        }

    }

    private static double calPearson(int size, double dotProduct, double rating1Sum, double rating2Sum, double rating1Norm, double rating2Norm) {
        return (size*dotProduct - rating1Sum*rating2Sum)/Math.sqrt(size*rating1Norm-rating1Sum*rating1Sum)/Math.sqrt(size*rating2Norm-rating2Sum*rating2Sum);
    }

    private static double calConsine(double dotProduct, double rating1Norm, double rating2Norm) {
        return dotProduct/Math.sqrt(rating1Norm)/Math.sqrt(rating2Norm);
    }

    private static double calJaccard(int size, int maxCount1, int maxCount2) {
        return ((double) size)/(maxCount1 + maxCount2 - size);
    }

}
