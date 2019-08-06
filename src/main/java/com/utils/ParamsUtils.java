package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created with IDEA by ChouFy on 2019/8/5.
 *
 * @author Zhoufy
 */
public class ParamsUtils {


    public static String JAR_PATH = "/Users/zhoufy/Documents/ideaProject/hadoop-case/out/artifacts/com_hadoop_jar/com.hadoop.jar";

    public static Job pretreatment(String[] args , int paramsLength) throws Exception {
        if (args.length != paramsLength && args.length < 2) {
            throw new Exception("args length error");
        }

        String input = args[0];
        String out = args[1];

        Configuration configuration = new Configuration();

        Path path = new Path(out);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(path)) {
            fs.deleteOnExit(path);
        }
        fs.close();

        configuration.addResource("hdfs-site.xml");
        configuration.addResource("core-site.xml");
        configuration.addResource("mapred-site.xml");
        configuration.addResource("yarn-site.xml");

        // 如果要从windows系统中运行这个job提交客户端的程序，则需要加这个跨平台提交的参数
        configuration.set("mapreduce.app-submission.cross-platform","true");



        Job job = Job.getInstance(configuration);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out));

        return job;
    }
}
