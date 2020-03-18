package com.leetcode;

/**
 * Created with IDEA by ChouFy on 2019/11/26.
 *
 * @author Zhoufy
 */
public class Difficult {

    public static double findMedianSortedArrays(int[] nums1, int[] nums2) {
        // 为了让搜索范围更小，我们始终让 num1 是那个更短的数组，
        if (nums1.length > nums2.length) {
            int[] temp = nums1;
            nums1 = nums2;
            nums2 = temp;
        }
        // 上述交换保证了 m <= n，在更短的区间 [0, m] 中搜索，会更快一些
        int m = nums1.length;
        int n = nums2.length;
        // 使用二分查找算法在数组 nums1 中搜索一个索引 i
        int left = 0;
        int right = m;
        while (left <= right) {
            int i = (left + right) / 2;
            int j = ((m + n + 1) / 2) - i;

            int nums1LeftMax = i == 0 ? Integer.MIN_VALUE : nums1[i - 1];
            int nums1RightMin = i == m ? Integer.MAX_VALUE : nums1[i];

            int nums2LeftMax = j == 0 ? Integer.MIN_VALUE : nums2[j - 1];
            int nums2RightMin = j == n ? Integer.MAX_VALUE : nums2[j];

            if (nums1LeftMax <= nums2RightMin && nums2LeftMax <= nums1RightMin) {
                if (((m + n) % 2) == 1){
//                if(m + n % 2 == 0){
                    /*
                        按位与运算符（&）
                        参加运算的两个数据，按二进制位进行“与”运算。
                        运算规则：0&0=0;   0&1=0;    1&0=0;     1&1=1;
                        即：两位同时为“1”，结果才为“1”，否则为0
                        例如：3&5  即 0000 0011 & 0000 0101 = 0000 0001   因此，3&5的值得1。
                     */
                    return Math.max(nums1LeftMax, nums2LeftMax);
                } else {
                    return (double) ((Math.max(nums1LeftMax, nums2LeftMax) + Math.min(nums1RightMin, nums2RightMin))) / 2;
                }
            } else if (nums2LeftMax > nums1RightMin) {
                left = i + 1;
            } else {
                right = i - 1;
            }
        }
        throw new IllegalArgumentException("传入无效的参数，输入的数组不是有序数组，算法失效");
    }


    public static void main(String[] args) {
        double result = Difficult.findMedianSortedArrays(new int[]{1, 2}, new int[]{3, 4});
        System.out.println(result);


        System.out.println(4 & 1);
        System.out.println(4 % 2);

    }
}
