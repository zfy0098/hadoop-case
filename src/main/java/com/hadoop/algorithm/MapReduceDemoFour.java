package com.hadoop.algorithm;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Created with IDEA by ChouFy on 2019/7/23.
 *
 *      左外链接
 * @author Zhoufy
 */
public class MapReduceDemoFour {


    public static class UserMapper extends Mapper<LongWritable, Text, PairOfStrings , Text>{


    }
}
