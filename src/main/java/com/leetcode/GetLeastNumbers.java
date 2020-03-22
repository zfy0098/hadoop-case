package com.leetcode;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created with IntelliJ IDEA by ChouFy on 2020-03-20.
 *
 * @author zhoufy
 */
public class GetLeastNumbers {


    /**
     *
     *   输入整数数组 arr ，找出其中最小的 k 个数。例如，输入4、5、1、6、2、7、3、8这8个数字，则最小的4个数字是1、2、3、4。
     *
     *
     *
     * 示例 1：
     *
     * 输入：arr = [3,2,1], k = 2
     * 输出：[1,2] 或者 [2,1]
     * 示例 2：
     *
     * 输入：arr = [0,1,2,1], k = 1
     * 输出：[0]
     *
     *
     * @param arr
     * @param k
     * @return
     */
    public int[] getLeastNumbers(int[] arr, int k) {
        if(arr == null){
            return null;
        }

        PriorityQueue<Integer> q = new PriorityQueue<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
               return Integer.compare(o2, o1);
            }
        });

        for (int i = 0; i < arr.length; i++) {
            if(q.size() < k || arr[i] < q.peek()){
                q.offer(arr[i]);
            }
            if(q.size() > k){
                q.poll();
            }
        }

        int[] nums = new int[k];

        int j = 0;
        for (int x : q) {
            nums[j++] = x;
        }

        return nums;
    }


    public static void main(String[] args) {
        GetLeastNumbers getLeastNumbers = new GetLeastNumbers();

        int[] arr = new int[]{2,49,5,3,7,34,25,16};
        int[] leastNumbers = getLeastNumbers.getLeastNumbers(arr, 2);
        for (int i = 0; i < leastNumbers.length; i++) {
            System.out.println(leastNumbers[i]);
        }
    }
}
