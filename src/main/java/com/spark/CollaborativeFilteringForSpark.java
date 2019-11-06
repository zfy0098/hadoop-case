package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2019/10/30.
 *
 * @author Zhoufy
 */
public class CollaborativeFilteringForSpark implements Serializable {


    private static final String OUTPUT_ONE = "/Users/zhoufy/Desktop/stepOne";

    private static final String OUTPUT_TWO = "/Users/zhoufy/Desktop/stepTwo";

    private static final String OUTPUT_THREE = "/Users/zhoufy/Desktop/stepThree";

    private void collaborativeFilterStepOne() {
        SparkConf conf = new SparkConf();
        conf.setAppName("collaborativeFilter");
        conf.setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("/Users/zhoufy/Desktop/cb_train.data");
        JavaPairRDD<String, Tuple2<String, Double>> userRdd = rdd.filter(text -> {
            try {
                return text.split(",").length == 3;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        }).mapToPair(line -> {
            String[] data = line.split(",");

            String user = data[0];
            String item = data[1];
            String score = data[2];

            Tuple2<String, String> tuple = new Tuple2<>(user, score);
            return new Tuple2<>(item, tuple);
        }).groupByKey().flatMapToPair(tuple -> {
            String item = tuple._1;
            Iterable<Tuple2<String, String>> values = tuple._2;

            double sum = 0.0;

            List<Map<String, Double>> list = new ArrayList<>();

            for (Tuple2<String, String> tuple2 : values) {
                String user = tuple2._1;
                String score = tuple2._2;
                sum += Math.pow(Double.parseDouble(score), 2);

                Map<String, Double> map = new HashMap<>(4);
                map.put(user, Double.parseDouble(score));
                list.add(map);
            }

            sum = Math.sqrt(sum);

            List<Tuple2<String, Tuple2<String, Double>>> results = new ArrayList<>();

            for (Map<String, Double> map : list) {
                for (Map.Entry<String, Double> entry : map.entrySet()) {
                    String user = entry.getKey();
                    Double score = entry.getValue();
                    BigDecimal b1 = new BigDecimal(score);
                    BigDecimal b2 = new BigDecimal(String.valueOf(sum));
                    Tuple2<String, Double> t2 = new Tuple2<>(item, b1.divide(b2, 2, 4).doubleValue());
                    results.add(new Tuple2<>(user, t2));
                }
            }
            return results.iterator();
        });

        List<Tuple2<String, Tuple2<String, Double>>> l = userRdd.collect();
        for (Tuple2<String, Tuple2<String, Double>> tuple : l) {
            System.out.println(tuple._1 + "\t" + tuple._2._1 + "\t" + tuple._2._2);
        }
        userRdd.repartition(1).saveAsTextFile(OUTPUT_ONE);
        sc.close();
    }


    private void stepTwo() {
        SparkConf conf = new SparkConf();
        conf.setAppName("collaborativeFilterStepTwo");
        conf.setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(OUTPUT_ONE + "/part-00000");

        rdd.filter(line -> line.split(",").length == 3).mapToPair(line -> {
            String[] values = line.split(",");
            String user = values[0].replace("(", "").replace(")", "");
            String item = values[1].replace("(", "").replace(")", "");
            String score = values[2].replace("(", "").replace(")", "");

            Tuple2<String, String> tuple = new Tuple2<>(item, score);
            return new Tuple2<>(user, tuple);
        }).groupByKey().flatMapToPair(line -> {
            Iterable<Tuple2<String, String>> tuple2 = line._2;
            List<Tuple2<String, String>> list = new ArrayList<>();
            for (Tuple2<String, String> t2 : tuple2) {
                list.add(t2);
            }

            Map<String, List<Tuple2<String, Double>>> map = new HashMap<>(16);
            for (int i = 0; i < list.size() - 1; i++) {
                for (int j = i + 1; j < list.size(); j++) {
                    Tuple2<String, String> t1 = list.get(i);
                    Tuple2<String, String> t2 = list.get(j);
                    String itemA = t1._1;
                    String itemAScore = t1._2;
                    String itemB = t2._1;
                    String itemBScore = t2._2;

                    BigDecimal b1 = new BigDecimal(itemAScore);
                    BigDecimal b2 = new BigDecimal(itemBScore);
                    double s = b1.multiply(b2).doubleValue();

                    if (map.get(itemA) == null) {
                        List<Tuple2<String, Double>> l = new ArrayList<>();
                        l.add(new Tuple2<>(itemB, s));
                        map.put(itemA, l);
                    } else {
                        List<Tuple2<String, Double>> l = map.get(itemA);
                        l.add(new Tuple2<>(itemB, s));

                        if (l.size() % 20 == 0) {
                            Collections.sort(l, (o1, o2) -> o1._2.compareTo(o2._2));
                            int maxLength = l.size() > 20 ? 20 : l.size();
                            l = l.subList(0, maxLength);
                            map.put(itemA, l);
                        }
                    }
                    if (map.get(itemB) == null) {
                        List<Tuple2<String, Double>> l = new ArrayList<>();
                        l.add(new Tuple2<>(itemA, s));
                        map.put(itemB, l);
                    } else {
                        List<Tuple2<String, Double>> l = map.get(itemB);
                        l.add(new Tuple2<>(itemA, s));
                        if (l.size() % 20 == 0) {
                            Collections.sort(l, (o1, o2) -> o1._2.compareTo(o2._2));
                            int maxLength = l.size() > 20 ? 20 : l.size();
                            l = l.subList(0, maxLength);
                            map.put(itemB, l);
                        }
                    }
                }
            }
            List<Tuple2<String, Tuple2<String, Double>>> results = new ArrayList<>();
            for (Map.Entry<String, List<Tuple2<String, Double>>> entry : map.entrySet()) {
                String item = entry.getKey();
                List<Tuple2<String, Double>> l = entry.getValue();
                for (Tuple2<String, Double> t : l) {
                    results.add(new Tuple2<>(item, t));
                }
            }
            return results.iterator();
        }).repartition(1).saveAsTextFile(OUTPUT_TWO);
        sc.close();
    }


    private void stepThree() {
        SparkConf conf = new SparkConf();
        conf.setAppName("collaborativeFilterStepThree");
        conf.setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(OUTPUT_TWO + "/part-00000");

        rdd.filter(line -> line.split(",").length == 3).mapToPair(line -> {
            String[] values = line.split(",");
            String itemA = values[0].replace("(", "").replace(")", "");
            String itemB = values[1].replace("(", "").replace(")", "");
            String score = values[2].replace("(", "").replace(")", "");

            return new Tuple2<>(itemA + "\001" + itemB, score);
        }).reduceByKey((t1, t2) -> (Double.parseDouble(t1) + Double.parseDouble(t2)) + "").repartition(1).saveAsTextFile(OUTPUT_THREE);
        sc.close();
    }


    public static void main(String[] args) {

        CollaborativeFilteringForSpark collaborativeFilteringForSpark = new CollaborativeFilteringForSpark();
        collaborativeFilteringForSpark.collaborativeFilterStepOne();
        collaborativeFilteringForSpark.stepTwo();
        collaborativeFilteringForSpark.stepThree();
    }

}
