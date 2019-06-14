package com.spark.algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

class Demo2 {

    private Demo2(){

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("demo2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD javaRDD = sc.textFile("/Users/zhoufy/sparktest/demo2");

    }

    public static void main(String[] args) {
        new Demo2();
    }

}
