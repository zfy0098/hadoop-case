package com.spark.algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2019/3/12.
 *
 * @author Zhoufy
 */
public class Demo14 implements Serializable {



    private static List<Tuple2<Tuple2<String, String>, String>> writableList(Map<Tuple2<String, String>, Double> pt) {
        List<Tuple2<Tuple2<String, String>, String>> list = new ArrayList<>();
        for (Map.Entry<Tuple2<String, String>, Double> entry : pt.entrySet()) {
            list.add(new Tuple2<>(new Tuple2<>(entry.getKey()._1, entry.getKey()._2),
                    entry.getValue().toString()));

        }
        return list;
    }


    private void stepOne() {
        String path = "/Users/zhoufy/sparktest/demo14";
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("朴树贝叶斯");
        sparkConf.setMaster("local");
        JavaSparkContext cxf = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = cxf.textFile(path, 1);
        rdd.saveAsTextFile("/Users/zhoufy/sparktest/out1/");
        long dataSize = rdd.count();
        JavaPairRDD<Tuple2<String, String>, Integer> pairs = rdd.flatMapToPair(s1 -> {
            List<Tuple2<Tuple2<String, String>, Integer>> list = new ArrayList<>();
            String[] tokens = s1.split(",");
            int cationIndex = tokens.length - 1;
            String cation = tokens[cationIndex];

            for (int i = 0; i < cationIndex; i++) {
                Tuple2<String, String> k = new Tuple2<>(tokens[i], cation);
                list.add(new Tuple2<>(k, 1));
            }
            Tuple2<String, String> k = new Tuple2<>("CLASS", cation);
            list.add(new Tuple2<>(k, 1));
            return list.iterator();
        });
        pairs.saveAsTextFile("/Users/zhoufy/sparktest/out2/");

        JavaPairRDD<Tuple2<String, String>, Integer> counts = pairs.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
        counts.saveAsTextFile("/Users/zhoufy/sparktest/out3/");

        Map<Tuple2<String, String>, Double> pt = new HashMap<>(16);
        List<String> classifications = new ArrayList<>();

        Map<Tuple2<String, String>, Integer> countAsMap = counts.collectAsMap();
        for (Map.Entry<Tuple2<String, String>, Integer> entry : countAsMap.entrySet()) {
            Tuple2<String, String> key = entry.getKey();
            String classification = key._2;
            if ("CLASS".equals(key._1)) {
                pt.put(key, ((double) entry.getValue()) / ((double) dataSize));
                classifications.add(classification);
            } else {
                Tuple2<String, String> key2 = new Tuple2<>("CLASS", classification);
                Integer count = countAsMap.get(key2);
                if (count == null) {
                    pt.put(key, 0.0);
                } else {
                    pt.put(key, ((double) entry.getValue()) / ((double) count));
                }
            }
        }

        JavaRDD<String> classificationsRdd = cxf.parallelize(classifications);
        classificationsRdd.saveAsTextFile("/Users/zhoufy/sparktest/class");

        List<Tuple2<Tuple2<String, String>, String>> list = writableList(pt);
        JavaRDD<Tuple2<Tuple2<String, String>, String>> ptrdd = cxf.parallelize(list);
        ptrdd.saveAsTextFile("/Users/zhoufy/sparktest/pt");
        cxf.close();
    }


    private void stepTwo() {


        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("step2");
        sparkConf.setMaster("local");
        JavaSparkContext cxf = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = cxf.textFile("/Users/zhoufy/sparktest/pt/", 1);

        JavaPairRDD<Tuple2<String, String>, Double> classifierRDD = rdd.mapToPair(s -> {
            String line = s.replace("(", "").replace(")", "");
            String[] lines = line.split(",");
            return new Tuple2<>(new Tuple2<>(lines[0].trim(), lines[1].trim()), Double.parseDouble(lines[2]));
        });

        Map<Tuple2<String, String>, Double> classifier = classifierRDD.collectAsMap();


        final Broadcast<Map<Tuple2<String, String>, Double>> broadcastClassifier = cxf.broadcast(classifier);

        JavaRDD<String> classesRDD = cxf.textFile("/Users/zhoufy/sparktest/class/", 1);
        List<String> classess = classesRDD.collect();
        final Broadcast<List<String>> broadcastClasses = cxf.broadcast(classess);

        JavaRDD<String> newData = cxf.textFile("/Users/zhoufy/sparktest/demo14.1", 1);

        JavaPairRDD<String, String> classified = newData.mapToPair(rec -> {
            // get the classifer from the cache
            Map<Tuple2<String, String>, Double> tuple2DoubleMap = broadcastClassifier.value();
            System.out.println("CLASSIFIER:" + tuple2DoubleMap.toString());
            // get the classes from the cache
            List<String> classes = broadcastClasses.value();

            // rec = (A1, A2, ..., Am)
            String[] attributes = rec.split(",");
            String selectedClass = null;
            double maxPosterior = 0.0;
            for (String aClass : classes) {
                double posterior = tuple2DoubleMap.get(new Tuple2<>("CLASS", aClass));
                System.out.println(posterior + "====" + aClass);
                for (String attribute :  attributes) {
                    Double probability = tuple2DoubleMap.get(new Tuple2<>(attribute, aClass));
                    if (probability == null) {
                        posterior = 0.0;
                        break;
                    } else {
                        posterior *= probability;
                    }
                }
                if (selectedClass == null) {
                    selectedClass = aClass;
                    maxPosterior = posterior;
                } else {
                    if (posterior > maxPosterior) {
                        selectedClass = aClass;
                        maxPosterior = posterior;
                    }
                }
            }
            return new Tuple2<>(rec, selectedClass);

        });
        classified.saveAsTextFile("/Users/zhoufy/sparktest/out10/");
    }


    public static void main(String[] args) {

        Demo14 demo14 = new Demo14();
        demo14.stepOne();
        demo14.stepTwo();

    }
}
