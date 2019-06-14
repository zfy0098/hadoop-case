package com;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created with IDEA by ChouFy on 2019/5/30.
 *
 * @author Zhoufy
 */
public class Test {
    public static void main(String[] args) {

        Integer[] a = new Integer[]{1559686109,
                1559696649,
                1559703923,
                1559706421,
                1559719327,
                1559729816,
                1559739102,
                1559739445,
                1559739466};

        ArrayList<Integer> list = new ArrayList<>(Arrays.asList(a));
        Collections.sort(list);

        int startTime = 0;
        int lastTime = 0;
        long onlineLong = 0;
        int threshold = 60;


        Collections.sort(list);

        int count = 0;
        for (int i = 0 ; i < list.size() ; i ++) {
            int t = list.get(i);
            if (startTime == 0) {
                startTime = t;
            }

            System.out.println("i: " + i + " ,t: " + t + " , startTime: " + startTime +  ", t - lastTime :" + (t - lastTime));


            if (startTime < t && t - lastTime <= threshold) {
                onlineLong += (t - lastTime);
                lastTime = t;
            } else {
                lastTime = t;
                count++;
            }
            System.out.println("i: " + i + " , onlineLong:" + onlineLong);
        }

        if (count > 0 && onlineLong < threshold) {
            if (count > 1) {
                count = count - 1;
            }
            onlineLong += count * threshold;
        }

        System.out.print(onlineLong);
    }
}
