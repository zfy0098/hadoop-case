package com.hadoop.leftouterjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.umd.cloud9.io.pair.PairOfStrings;
import java.util.Iterator;

/**
 * Created with IDEA by ChouFy on 2019/6/18.
 *
 * @author Zhoufy
 */
public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

    Text productID = new Text();
    Text locationID = new Text("undefined");

    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context)
            throws java.io.IOException, InterruptedException {


        Iterator<PairOfStrings> iterator = values.iterator();
        while (iterator.hasNext()){
            PairOfStrings pair = iterator.next();

            context.write(new Text(key.toString()), new Text(pair.toString()));
        }



//        Iterator<PairOfStrings> iterator = values.iterator();
//        if (iterator.hasNext()) {
//            // firstPair must be location pair
//            PairOfStrings firstPair = iterator.next();
//            System.out.println("firstPair="+firstPair.toString());
//            if (firstPair.getLeftElement().equals("L")) {
//                locationID.set(firstPair.getRightElement());
//            }
//        }
//
//        while (iterator.hasNext()) {
//            // the remaining elements must be product pair
//            PairOfStrings productPair = iterator.next();
//            System.out.println("productPair="+productPair.toString());
//            productID.set(productPair.getRightElement());
//            context.write(productID, locationID);
//        }
    }

}
