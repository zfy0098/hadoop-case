package com.leetcode;

import java.util.HashMap;
import java.util.Map;


/**
 * Created with IDEA by ChouFy on 2019/6/13.
 *
 * @author Zhoufy
 */
public class Solution {


    /**
     * 给定一个整数数组 nums 和一个目标值 target，请你在该数组中找出和为目标值的那 两个 整数，并返回他们的数组下标。
     * 你可以假设每种输入只会对应一个答案。但是，你不能重复利用这个数组中同样的元素
     * 给定 nums = [2, 7, 11, 15], target = 9
     * 因为 nums[0] + nums[1] = 2 + 7 = 9
     * 所以返回 [0, 1]
     *
     * @param nums
     * @param target
     * @return
     */
    public static int[] twoSum(int[] nums, int target) {
        int[] indexs = new int[2];
        Map<Integer, Integer> hashMap = new HashMap<>(6);
        for (int i = 0; i < nums.length; i++) {
            if (hashMap.containsKey(nums[i])) {
                indexs[0] = hashMap.get(nums[i]);
                indexs[1] = i;
                System.out.println("num[0] = " + indexs[0] + " , " + indexs[1]);
                return indexs;
            }
            System.out.println(target + " - " + indexs[i] + " = " + (target - indexs[i]) + " ， i = " + i);
            hashMap.put(target - nums[i], i);
        }
        return indexs;
    }

    /**
     * 给出两个 非空 的链表用来表示两个非负的整数。其中，它们各自的位数是按照 逆序 的方式存储的，并且它们的每个节点只能存储 一位 数字。
     * 如果，我们将这两个数相加起来，则会返回一个新的链表来表示它们的和。
     * 您可以假设除了数字 0 之外，这两个数都不会以 0 开头。
     * <p>
     * 示例：
     * <p>
     * 输入：(2 -> 4 -> 3) + (5 -> 6 -> 4)
     * 输出：7 -> 0 -> 8
     * 原因：342 + 465 = 807
     *
     * @param l1
     * @param l2
     * @return
     */
    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode root = new ListNode(0);
        ListNode currsor = root;
        int carry = 0;
        while (l1 != null || l2 != null || carry != 0) {
            int l1val = l1 != null ? l1.val : 0;
            int l2val = l2 != null ? l2.val : 0;
            int sumVal = l1val + l2val + carry;
            carry = sumVal / 10;
            ListNode sumNode = new ListNode(sumVal % 10);
            currsor.next = sumNode;
            currsor = sumNode;

            if (l1 != null) {
                l1 = l1.next;
            }
            if (l2 != null) {
                l2 = l2.next;
            }
        }
        return root.next;

    }


    /**
     *    给定一个字符串，请你找出其中不含有重复字符的 最长子串 的长度。
     *
     *    标签：滑动窗口
     *      暴力解法时间复杂度较高，会达到 O(n^2)O(n2)，故而采取滑动窗口的方法降低时间复杂度
     *      定义一个 map 数据结构存储 (k, v)，其中 key 值为字符，value 值为字符位置 +1，加 1 表示从字符位置后一个才开始不重复
     *      我们定义不重复子串的开始位置为 start，结束位置为 end
     *      随着 end 不断遍历向后，会遇到与 [start, end] 区间内字符相同的情况，此时将字符作为 key 值，获取其 value 值，并更新 start，此时 [start, end] 区间内不存在重复字符
     *      无论是否更新 start，都会更新其 map 数据结构和结果 ans。
     *      时间复杂度：O(n)O(n)

     * @param s
     * @return
     */
    public static int lengthOfLongestSubstring(String s) {
        int ans = 0;
        int start = 0;
        Map<Character, Integer> map = new HashMap<>();

        char[] buffer = s.toCharArray();
        for (int i = 0; i < buffer.length; i++) {
            if (map.containsKey(buffer[i])) {
                start = Math.max(map.get(buffer[i]), start);
            }
            map.put(buffer[i], i + 1);
            ans = Math.max(i - start + 1, ans);
            System.out.println(buffer[i] + ":" +ans);

        }

        return ans;
    }



    public static void main(String[] args) {
//        Solution.twoSum(new int[]{2, 7, 11, 15}, 9);
//        ListNode listNode = Solution.addTwoNumbers(new ListNode(2), new ListNode(3));
        int ans = Solution.lengthOfLongestSubstring("abcdefga");
        System.out.println(ans);


    }




}
