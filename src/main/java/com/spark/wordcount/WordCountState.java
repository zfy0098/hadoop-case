package com.spark.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created  with  IDEA  by  ChouFy  on  2019/6/7.
 *
 * @author Zhoufy
 */
public class WordCountState implements Serializable {

//        public  int  updateFunction(currentValues:  Seq[Int],  preValues:  Option[Int]):  Option[Int]  =  {
//                val  current  =  currentValues.sum
//                val  pre  =  preValues.getOrElse(0)
//                Some(current  +  pre)
//        }

    private void init() {
        SparkConf conf = new SparkConf().setAppName("wordCountState").setMaster("local[2]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        ssc.checkpoint("/Users/zhoufy/checkpoint");


        JavaDStream<String> lines = ssc.socketTextStream("master", 9999, StorageLevel.MEMORY_ONLY());

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));


        //  无状态 wordCount
//        JavaPairDStream<String,  Integer>  wordCounts  =    pairs.reduceByKey((integer  ,  integer1)  ->
//                  integer  +  integer1
//        );

        // 窗口数据
//        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        } ,  Durations.seconds(30), Durations.seconds(5));


        /**
         *  持续累加的wordCount
         */
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            /**
             *  serialVersionUID
             */

            //  参数valueList:相当于这个batch,这个key新的值，可能有多个,比如（hadoop,1）(hadoop,1)传入的可能是(1,1)
            //  参数oldState:就是指这个key之前的状态
            @Override
            public Optional<Integer> call(List<Integer> valueList, Optional<Integer> oldState) throws Exception {
                Integer newState = 0;
                //  如果oldState之前已经存在，那么这个key可能之前已经被统计过，否则说明这个key第一次出现
                if (oldState.isPresent()) {
                    newState = oldState.get();
                }

                //  更新state
                for (Integer value : valueList) {
                    newState += value;
                }
                return Optional.of(newState);
            }
        });

        wordCounts.print();

        try {
            ssc.start();
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        WordCountState wordCountState = new WordCountState();
        wordCountState.init();
    }
}
