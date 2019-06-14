package com.spark.wordcount;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * Created with IDEA by ChouFy on 2019/6/7.
 *
 * @author Zhoufy
 */
public class TestNewSparkTaskUpdateByKey {

    private static final String SOCKET_SERVER_IP = "master";

    private static final int SOCKET_SERVER_PORT = 9999;

    private static final String CHECK_POINT_DIR = "/Users/zhoufy/checkpoint";

    private static final int CHECK_POINT_DURATION_SECONDS = 5;

    private static JavaStreamingContext getJavaStreamingContext() {

        SparkConf conf = new SparkConf().setAppName("SparkUpdateStateByKey").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(CHECK_POINT_DURATION_SECONDS));

        jssc.checkpoint(CHECK_POINT_DIR);

        JavaReceiverInputDStream<String> messages = jssc.socketTextStream(SOCKET_SERVER_IP, SOCKET_SERVER_PORT);

        JavaDStream<String> words = messages.flatMap(new FlatMapFunction<String, String>() {

            /**
             * serialVersionUID
             */
            private static final long serialVersionUID = -8511938709723688992L;

            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            /**
             * serialVersionUID
             */
            private static final long serialVersionUID = 7494315448364736838L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        // 统计全局的word count，而不是单一的某一批次
        JavaPairDStream<String, Integer> wordcounts = pairs.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    /**
                     * serialVersionUID
                     */
                    private static final long serialVersionUID = -7837221857493546768L;

                    // 参数valueList:相当于这个batch,这个key新的值，可能有多个,比如（hadoop,1）(hadoop,1)传入的可能是(1,1)
                    // 参数oldState:就是指这个key之前的状态
                    @Override
                    public Optional<Integer> call(List<Integer> valueList,
                                                  Optional<Integer> oldState)
                            throws Exception {
                        Integer newState = 0;
                        // 如果oldState之前已经存在，那么这个key可能之前已经被统计过，否则说明这个key第一次出现
                        if (oldState.isPresent()) {
                            newState = oldState.get();
                        }

                        // 更新state
                        for (Integer value : valueList) {
                            newState += value;
                        }
                        return Optional.of(newState);
                    }
                });

        wordcounts.print();
        return jssc;
    }

    private static void testSpark() {
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(CHECK_POINT_DIR, new Function0<JavaStreamingContext>() {

            /**
             * serialVersionUID
             */
            private static final long serialVersionUID = -6070032440759098908L;

            @Override
            public JavaStreamingContext call() throws Exception {
                return getJavaStreamingContext();
            }

        });


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jssc.close();
    }

    public static void main(String[] args) {
        testSpark();
    }

}
