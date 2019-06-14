package com.spark.userwatch;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created with IDEA by ChouFy on 2019/6/10.
 *
 * @author Zhoufy
 */
public class UserWatch implements Serializable {

    public void userWatchList() {

        SparkConf conf = new SparkConf();
        conf.setAppName("userWatchList");
        conf.setMaster("local[8]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("/Users/zhoufy/Desktop/train_new.data");

        String out = "/opt/userwatchlist_result";
        rdd.filter(line -> {
            String[] x = line.split("\t");
            if (x.length == 3) {
                return Double.parseDouble(x[2]) > 2.0;
            }
            return false;
        }).mapToPair(line -> {
            String[] data = line.split("\t");
            String userid = data[0];
            String itemid = data[1];
            String score = data[2];

            return new Tuple2<>(userid, new Tuple2<>(itemid, score));
        }).groupByKey().mapToPair(x -> {

            String userid = x._1;
            Iterable<Tuple2<String, String>> iterable = x._2;

            List<Tuple2<String, String>> list = Lists.newArrayList(iterable);

            list.sort((o1, o2) ->
                    Integer.parseInt(o2._2) - Integer.parseInt(o1._2)
            );
            if (list.size() > 5) {
                list = list.subList(0, 5);
            }
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                stringBuilder.append(list.get(i)._1).append(":").append(list.get(i)._2).append(" ");
            }
            return new Tuple2<>(userid, stringBuilder.toString());
        }).distinct().saveAsTextFile(out);
    }

    public static void main(String[] args) {
        UserWatch userWatch = new UserWatch();
        userWatch.userWatchList();
    }
}
