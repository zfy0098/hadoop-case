package com.hadoop.leftouterjoin;

import org.apache.hadoop.mapreduce.Partitioner;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * Created with IDEA by ChouFy on 2019/6/19.
 *
 * @author Zhoufy
 */
public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object> {
    @Override
    public int getPartition(PairOfStrings key,
                            Object value,
                            int numberOfPartitions) {
        return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numberOfPartitions;
    }
}
