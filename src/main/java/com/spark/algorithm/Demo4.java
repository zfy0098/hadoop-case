package com.spark.algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Demo4 implements Serializable {

    private Demo4() {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("demo4");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String userPath = "/Users/zhoufy/sparktest/demo4user";
        String transacations = "/Users/zhoufy/sparktest/demo4transacations";

        JavaRDD<String> user = sc.textFile(userPath, 1);
        JavaPairRDD<String, Tuple2<String, String>> usersRdd = user.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                String[] tokens = s.split(",");
                Tuple2<String, String> localtion = new Tuple2<>("L", tokens[1]);
                return new Tuple2<>(tokens[0], localtion);
            }
        });

        JavaRDD<String> trans = sc.textFile(transacations);
        JavaPairRDD<String, Tuple2<String, String>> transRDD = trans.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                String[] transRecord = s.split(",");
                Tuple2<String, String> product = new Tuple2<>("P", transRecord[1]);
                return new Tuple2<>(transRecord[2], product);
            }
        });

        JavaPairRDD<String, Tuple2<String, String>> allRdd = transRDD.union(usersRdd);

        List<Tuple2<String,Tuple2<String,String>>> list = allRdd.collect();

        System.out.println("============  allrdd debug ==================");
        for (Tuple2<String, Tuple2<String,String>> tuple2: list) {
            System.out.println("tuple2._1 :"  + tuple2._1 +  "\ttuple2._2 :"  + tuple2._2);

        }
        System.out.println("============  allrdd debug  end ==================");

        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD = allRdd.groupByKey();

        JavaPairRDD<String, String> productLocalsRDD = groupedRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<Tuple2<String, String>>> s) throws Exception {

                Iterable<Tuple2<String, String>> pairs = s._2;

                String local = "UNKNOWN";
                List<String> products = new ArrayList<>();
                for (Tuple2<String, String> t : pairs) {
                    if (t._1.equals("L")) {
                        local = t._2;
                    } else {
                        products.add(t._2);
                    }
                }

                List<Tuple2<String, String>> kvlist = new ArrayList<>();
                for (String product : products) {
                    kvlist.add(new Tuple2<String, String>(product, local));
                }

                return kvlist.iterator();
            }
        });

        JavaPairRDD<String, Iterable<String>> productBylocations = productLocalsRDD.groupByKey();

        JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniquelocations = productBylocations.mapValues(new Function<Iterable<String>, Tuple2<Set<String>, Integer>>() {
            @Override
            public Tuple2<Set<String>, Integer> call(Iterable<String> s) throws Exception {

                Set<String> set = new HashSet<>();
                for (String local : s) {
                    set.add(local);
                }
                return new Tuple2<Set<String>, Integer>(set, set.size());
            }
        });

        List<Tuple2<String, Tuple2<Set<String> , Integer>>> debug = productByUniquelocations.collect();

        for (Tuple2 t2 : debug) {
            System.out.print("t2._1 : " + t2._1);
            System.out.println("t2._2 : " + t2._2);
        }
    }

    public static void main(String[] args) {
        new Demo4();
    }
}
