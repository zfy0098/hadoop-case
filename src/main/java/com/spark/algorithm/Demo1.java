package com.spark.algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class Demo1 implements Serializable {

    private Demo1() {
        String path = "/Users/zhoufy/sparktest/demo1";

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("demo1");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD javaRDD = ctx.textFile(path);
        JavaPairRDD javaPairRDD = javaRDD.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
                String[] tokens = s.split(",");
                System.out.println(tokens[0] + " , " + tokens[1] + " , " + tokens[2]);
                Integer time = Integer.parseInt(tokens[1]);
                Integer value = Integer.parseInt(tokens[2]);
                Tuple2<Integer, Integer> tuple2 = new Tuple2<>(time, value);
                return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0], tuple2);
            }
        });

        List<Tuple2<String, Tuple2<Integer, Integer>>> output1 = javaPairRDD.collect();
        for (Tuple2 tuple2 : output1) {
            Tuple2<Integer, Integer> timeValue = (Tuple2<Integer, Integer>) tuple2._2;
            System.out.println(tuple2._1 + ", " + timeValue._1 + ", " + timeValue._2);
        }

        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = javaPairRDD.groupByKey();
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 = groups.collect();

        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output2) {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : list) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("========================");
        }


        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted = groups.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>() {
            @Override
            public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> s) throws Exception {
                List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>();

                for (Tuple2<Integer, Integer> t : s) {
                    newList.add(t);
                }

                Collections.sort(newList, new Comparator<Tuple2<Integer, Integer>>() {
                    @Override
                    public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                        return o1._1.compareTo(o2._1);
                    }
                });
                return newList;
            }
        });


        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output3 = sorted.collect();

        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output3) {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : list) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("========");
        }
    }


    public static void main(String[] args) {

        new Demo1();

    }
}
