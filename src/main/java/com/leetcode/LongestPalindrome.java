package com.leetcode;

/**
 * Created with IntelliJ IDEA by ChouFy on 2020-03-20.
 *
 * @author zhoufy
 */
public class LongestPalindrome {


    public static int longestPalindrome(String s) {


        int[] count = new int[128];
        for (char c: s.toCharArray()){
            count[c]++;
        }

        int ans = 0;
        for (int v: count) {

            /*
            对于每个字符 ch，假设它出现了 v 次，
            我们可以使用该字符 v / 2 * 2 次，
            在回文串的左侧和右侧分别放置 v / 2 个字符 ch，其中 / 为整数除法。
            例如若 "a" 出现了 5 次，那么我们可以使用 "a" 的次数为 4，回文串的左右两侧分别放置 2 个 "a"。

            如果有任何一个字符 ch 的出现次数 v 为奇数（即 v % 2 == 1），
            那么可以将这个字符作为回文中心，
            注意只能最多有一个字符作为回文中心。
            在代码中，我们用 ans 存储回文串的长度，
            由于在遍历字符时，ans 每次会增加 v / 2 * 2，
            因此 ans 一直为偶数。但在发现了第一个出现次数为奇数的字符后，
            我们将 ans 增加 1，这样 ans 变为奇数，在后面发现其它出现奇数次的字符时，我们就不改变 ans 的值了。

             */
            ans += v / 2 * 2;
            // ans 一直是偶数的存在
            // v 发现第一奇数 那么 ans + 1 。 ans 变成了奇数 那么 后边的奇数 不会改变ans的值
            if (v % 2 == 1 && ans % 2 == 0){
                ans++;
            }
        }
        return ans;

        //  第二种方案
//        int[] cnt = new int[58];
//        for (char c : s.toCharArray()) {
//            System.out.println(c - 'A');
//            cnt[c - 'A'] += 1;
//        }
//
//        int ans = 0;
//        for (int x : cnt) {
//            // 字符出现的次数最多用偶数次。
//            // 如果是偶数的话二进制末尾为0，&0的结果就是0，同理奇数的时候为1
//            ans += x - (x & 1);
//            System.out.println("x : " + x + "\t:" + (x & 1) + " \t ， " + ans);
//
//        }
//        // 如果最终的长度小于原字符串的长度，说明里面某个字符出现了奇数次，那么那个字符可以放在回文串的中间，所以额外再加一。
//        return ans < s.length() ? ans + 1 : ans;



        //  第三种方案

//        if(s == null){
//            return 0;
//        }
//        char[] chars = s.toCharArray();
//        int[] hash = new int[58];
//        for(char ch : chars){
//            hash[ch - 'A'] += 1;
//        }
//
//        int count = 0;
//        for(int i = 0; i < hash.length; i++){
//            if(hash[i] % 2 == 0){
//                //  偶数情况下 直接加个数  4 2对 直接加4
//                count += hash[i];
//            }else
//                //  奇数 减一 变成偶数或者0  1-1 = 0  3-1 = 2
//                count += --hash[i];
//            }
//        }
//
//        if(s.length() > count){
//            count++;
//        }
//
//        return count;

    }

}
