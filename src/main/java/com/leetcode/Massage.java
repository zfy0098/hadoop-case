package com.leetcode;

/**
 * Created with IntelliJ IDEA by ChouFy on 2020-03-24.
 * <p>
 * 按摩师
 *
 * @author zhoufy
 */
public class Massage {


    public int massage(int[] nums) {

        // 1 2 3 4

        int[] dp = new int[nums.length];
        dp[0] = nums[0];
        dp[1] = nums[1] > nums[0] ? nums[1] : nums[0];

        for (int i = 2; i < nums.length; i++) {
            dp[i] = Math.max(dp[i - 1], dp[i - 2] + nums[i]);
        }
        return dp[nums.length - 1];
    }


    public static void main(String[] args) {
        Massage massage = new Massage();

        int massage1 = massage.massage(new int[]{1, 1, 2, 3, 1,10});
        System.out.println(massage1);
    }
}
