package com.spark.algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple7;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IDEA by ChouFy on 2019/3/20.
 *
 * @author Zhoufy
 */
public class Demo10 implements Serializable {


    private void init() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("基于内容的电影推荐");
        JavaSparkContext cxf = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = cxf.textFile("/Users/zhoufy/sparktest/demo10", 1);
        JavaPairRDD<String, Tuple2<String, Integer>> rdd = javaRDD.mapToPair(line -> {
            String[] values = line.split(",");
            // 电影名称
            String movie = values[1];
            // 用户
            String user = values[0];
            // 评分
            String rating = values[2];
            return new Tuple2<>(movie, new Tuple2<>(user, Integer.parseInt(rating)));
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> moviesGrouped = rdd.groupByKey();

        List<Tuple2<String, Iterable<Tuple2<String, Integer>>>> debug1 = moviesGrouped.collect();
        for (Tuple2<String, Iterable<Tuple2<String, Integer>>> tu : debug1) {
            System.out.println("debug1  tu._1 : " + tu._1);
            System.out.println("debug1  tu,_2 : " + tu._2);
            System.out.println("-------------------");
        }

        JavaPairRDD<String, Tuple3<String, Integer, Integer>> userRDD = moviesGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Tuple3<String, Integer, Integer>>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                String movie = stringIterableTuple2._1;
                Iterable<Tuple2<String, Integer>> pairsOfUserAndRating = stringIterableTuple2._2;
                int number = 0;
                for (Tuple2<String, Integer> tuple2 : pairsOfUserAndRating) {
                    number++;
                    list.add(tuple2);
                }
                List<Tuple2<String, Tuple3<String, Integer, Integer>>> results = new ArrayList<>();

                // now emit (K, V) pairs
                for (Tuple2<String, Integer> t2 : list) {
                    // 用户
                    String user = t2._1;
                    //  用户针对电影的评分
                    Integer rating = t2._2;
                    // number  针对这部电影评分的人数
                    Tuple3<String, Integer, Integer> t3 = new Tuple3<>(movie, rating, number);
                    results.add(new Tuple2<>(user, t3));
                }
                return results.iterator();
            }
        });


        List<Tuple2<String, Tuple3<String, Integer, Integer>>> userRDDList = userRDD.collect();
        for (Tuple2<String, Tuple3<String, Integer, Integer>> tuple2 : userRDDList) {
            //  打印结果 用户 电影  评分  评分人数
            System.out.println("userRDDList   debug 3 -- " + tuple2._1 + "--- values :" + tuple2._2 + " , " + tuple2._2._1() + "\t" + tuple2._2._2() + "\t" + tuple2._2._3() + "\t");
        }

        JavaPairRDD<String, Iterable<Tuple3<String, Integer, Integer>>> groupedByUser = userRDD.groupByKey();

        List<Tuple2<String, Iterable<Tuple3<String, Integer, Integer>>>> debug4 = groupedByUser.collect();
        for (Tuple2<String, Iterable<Tuple3<String, Integer, Integer>>> t2 : debug4) {
            System.out.println("debug4 key=" + t2._1 + "\t value=" + t2._2);
        }

        JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> joinRdd = userRDD.join(userRDD);

        List<Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>>> debug5 = joinRdd.collect();

        for (Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> t2 : debug5) {
            System.out.println("debug5 , key : +" + t2._1 + " , values :" + t2._2);
        }

        JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> filteredRDD = joinRdd.filter(s -> {
            Tuple3<String, Integer, Integer> movie1 = s._2._1;
            Tuple3<String, Integer, Integer> movie2 = s._2._2;

            String movieName1 = movie1._1();
            String movieName2 = movie2._1();
            return movieName1.compareTo(movieName2) < 0;
        });


        List<Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>>> debug55 = filteredRDD.collect();

        for (Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> t2 : debug55) {
            System.out.println("debug55 , key : +" + t2._1 + " , values :" + t2._2);
        }

        JavaPairRDD<Tuple2<String, String>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> moviePair = filteredRDD.mapToPair(s -> {

            Tuple3<String, Integer, Integer> movie1 = s._2._1;
            Tuple3<String, Integer, Integer> movie2 = s._2._2;
            Tuple2<String, String> m1m2key = new Tuple2<>(movie1._1(), movie2._1());

            int ratingProduct = movie1._2() * movie2._2();
            int rating1Squared = movie1._2() * movie1._2();
            int rating2Squared = movie2._2() * movie2._2();

            Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple7 = new Tuple7<>(movie1._2(),
                    movie1._3(), movie2._2(), movie2._3(), ratingProduct, rating1Squared, rating2Squared);
            return new Tuple2<>(m1m2key, tuple7);
        });


        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>> corrRDD = moviePair.groupByKey();

        JavaPairRDD<Tuple2<String, String>, Tuple3<Double, Double, Double>> corr = corrRDD.mapValues(s -> calculateCorrelations(s));

        corr.saveAsTextFile("/Users/zhoufy/sparktest/demo10out/");
    }


    private static Tuple3<Double, Double, Double> calculateCorrelations(Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> values) {
        // 各个向量的长度
        int groupSize = 0;
        // ratingProd 之和
        int dotProduct = 0;
        //  rating1 之和
        int rating1Sum = 0;
        //  rating2 之和
        int rating2Sum = 0;

        int rating1NormSq = 0;
        int rating2NormSq = 0;

        int maxNumOfRaterS1 = 0;
        int maxNumOfRaterS2 = 0;

        for (Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> t7 : values) {
            groupSize++;
            dotProduct += t7._5();
            rating1Sum += t7._1();
            rating2Sum += t7._3();
            rating1NormSq += t7._6();
            rating2NormSq += t7._7();

            int numOfRaterS1 = t7._2();
            if (numOfRaterS1 > maxNumOfRaterS1) {
                maxNumOfRaterS1 = numOfRaterS1;
            }

            int numOfRaterS2 = t7._4();

            if (numOfRaterS2 > maxNumOfRaterS2) {
                maxNumOfRaterS2 = numOfRaterS2;
            }
        }

        double pearson = calculatePearsonCorrelation(groupSize, dotProduct, rating1Sum, rating2Sum, rating1NormSq, rating2NormSq);

        double cosine = calculateCosineCorrelation(groupSize, Math.sqrt(rating1NormSq), Math.sqrt(rating2NormSq));

        double jaccard = calculateJaccardCorrelation(groupSize, maxNumOfRaterS1, maxNumOfRaterS2);

        return new Tuple3<>(pearson, cosine, jaccard);
    }


    private static double calculatePearsonCorrelation(double size, double dotProduct, double rating1Sum, double rating2Sum,
                                                      double rating1NormSq, double rating2NormSq) {
        double numerator = size * dotProduct - rating1Sum * rating2Sum;
        double denominator = Math.sqrt(size * rating1NormSq - rating1Sum * rating1Sum) * Math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum);
        return numerator / denominator;
    }


    private static double calculateCosineCorrelation(double dotProduct, double rating1Norm, double rating2Norm) {
        return dotProduct / (rating1Norm * rating2Norm);
    }

    private static double calculateJaccardCorrelation(double inCommon, double totalA, double totalB) {
        double union = totalA + totalB - inCommon;
        return inCommon / union;
    }

    public static void main(String[] args) {
        Demo10 demo10 = new Demo10();
        demo10.init();
    }
}
