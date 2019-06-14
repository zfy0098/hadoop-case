package com.spark.algorithm;

import com.utils.Combination;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.awt.event.HierarchyBoundsAdapter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 购物篮分析
 */
public class Demo7 {

    private static List<String> toList(String transaction) {
        String[] items = transaction.split(",");
        return new ArrayList<>(Arrays.asList(items));
    }

    private static List<String> removeOneItem(List<String> list, int i) {
        if ((list == null) || list.isEmpty()) {
            return list;
        } else if (i < 0 || i > list.size() - 1) {
            return list;
        }

        List<String> cloned = new ArrayList<>(list);
        cloned.remove(i);
        return cloned;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("demo7");

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.serializer.buffer.mb", "32");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/zhoufy/sparktest/demo7");


        JavaPairRDD<List<String>, Integer> patterns = lines.flatMapToPair(new PairFlatMapFunction<String, List<String>, Integer>() {
            @Override
            public Iterator<Tuple2<List<String>, Integer>> call(String s) throws Exception {

                List<String> list = toList(s);
                List<List<String>> combinations = Combination.findSortedCombinations(list);

                List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
                for (List<String> combList : combinations) {
                    if (combList.size() > 0) {
                        result.add(new Tuple2<>(combList, 1));
                    }
                }
                return result.iterator();
            }
        });


        JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });


        JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subpatterns = combined.flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>, Integer>, List<String>, Tuple2<List<String>, Integer>>() {
            @Override
            public Iterator<Tuple2<List<String>, Tuple2<List<String>, Integer>>> call(Tuple2<List<String>, Integer> pattern) throws Exception {


                List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<>();


                List<String> list = pattern._1;
                Integer frequency = pattern._2;

                result.add(new Tuple2<>(list, new Tuple2<>(null, frequency)));

                if (list.size() == 1) {
                    return result.iterator();
                }

                for (int i = 0; i < list.size(); i++) {
                    List<String> sublist = removeOneItem(list, i);
                    result.add(new Tuple2<>(sublist, new Tuple2<>(list, frequency)));
                }

                return result.iterator();
            }
        });


        JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subpatterns.groupByKey();


        JavaRDD<List<Tuple3<List<String>, List<String>, Double>>> assocRules = rules.map(new Function<Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>>, List<Tuple3<List<String>, List<String>, Double>>>() {
            @Override
            public List<Tuple3<List<String>, List<String>, Double>> call(Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>> in) throws Exception {

                List<Tuple3<List<String>, List<String>, Double>> result = new ArrayList<>();

                List<String> fromlist = in._1;
                Iterable<Tuple2<List<String>, Integer>> to = in._2;
                List<Tuple2<List<String>, Integer>> toList = new ArrayList<>();
                Tuple2<List<String>, Integer> fromCount = null;
                for (Tuple2<List<String>, Integer> t2 : to) {
                    if (t2._1 == null) {
                        fromCount = t2;
                    } else {
                        toList.add(t2);
                    }
                }

                if (toList.isEmpty()) {
                    return result;
                }

                for (Tuple2<List<String>, Integer> t2 : toList) {
                    Double confidence = (double) t2._2 / (double) fromCount._2;
                    List<String> t2list = new ArrayList<>();
                    t2list.removeAll(fromlist);
                    result.add(new Tuple3<>(fromlist, t2list, confidence));
                }

                return result;
            }
        });


        assocRules.saveAsTextFile("/Users/zhoufy/Documents/ideaProject/sparkdemo/out/");


    }
}
