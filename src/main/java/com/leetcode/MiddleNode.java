package com.leetcode;


/**
 * Created with IntelliJ IDEA by ChouFy on 2020-03-23.
 *
 * @author zhoufy
 */
public class MiddleNode implements java.io.Serializable {


    class ListNode {
        Integer val;
        ListNode next;

        ListNode(int x) {
            System.out.println("=======45:" + x);
            this.val = x;
        }

        public ListNode(int[] nums) {
            if (nums == null || nums.length == 0) {
                throw new IllegalArgumentException("arr can not be empty");
            }
            this.val = nums[0];
            ListNode current = this;
            for (int i = 1; i < nums.length; i++) {
                current.next = new ListNode(nums[i]);
                current = current.next;
            }

            System.out.println(current.val + " , " + val + " ----" + next.val);
        }

    }

    public ListNode middleNode(ListNode head) {
        if (head == null) {
            return null;
        }
        ListNode slow = head;
        ListNode fast = head;

        while (fast != null && fast.next != null) {
            slow = slow.next;
            fast = fast.next.next;
        }
        return slow;
    }


    public static void main(String[] args) {
        int[] arr = new int[]{1, 2, 3, 4, 5};
        MiddleNode middleNode = new MiddleNode();
        ListNode head = middleNode.new ListNode(arr);

        System.out.println(head.val);
        head = head.next;
        while (head != null) {
            System.out.println(head.val);
            head = head.next;
        }


//        MiddleNode solution = new MiddleNode();
//        ListNode res = solution.middleNode(head);
//        System.out.println(res.val);
//        System.out.println(res.next.next.val);
    }
}

