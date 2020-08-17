package com.leetcode;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA by ChouFy on 2020-03-28.
 *
 *
 * 给定一个单词列表，我们将这个列表编码成一个索引字符串 S 与一个索引列表 A。
 *
 * 例如，如果这个列表是 ["time", "me", "bell"]，我们就可以将其表示为 S = "time#bell#" 和 indexes = [0, 2, 5]。
 *
 * 对于每一个索引，我们可以通过从字符串 S 中索引的位置开始读取字符串，直到 "#" 结束，来恢复我们之前的单词列表。
 *
 * 那么成功对给定单词列表进行编码的最小字符串长度是多少呢？
 *
 * 来源：力扣（LeetCode）
 * 链接：https://leetcode-cn.com/problems/short-encoding-of-words
 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 *
 * @author zhoufy
 */
public class MinimumLengthEncoding {


    public int minimumLengthEncoding(String[] words){

        String[] reverse = new String[words.length];
        for (int i = 0; i < words.length; i++){
            reverse[i] = new StringBuilder(words[i]).reverse().toString();
        }

        Arrays.sort(reverse);

        int ret = 0;
        for(int i = 0; i < words.length; i++){
            if( i + 1 < words.length && reverse[i + 1].startsWith(reverse[i])){
                // 第一个单词 是 第二个词的开头
            } else {
                ret += reverse[i].length() + 1;
            }
        }


        return ret;
    }



    public static void main(String[] args) {

        MinimumLengthEncoding minimumLengthEncoding = new MinimumLengthEncoding();
        int i = minimumLengthEncoding.minimumLengthEncoding(new String[]{"time", "lltme", "aaaa"});
        System.out.println(i);
    }
}
