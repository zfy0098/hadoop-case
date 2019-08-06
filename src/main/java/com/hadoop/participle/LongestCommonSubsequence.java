package com.hadoop.participle;

import static java.lang.Math.max;

/**
 * Created with IDEA by ChouFy on 2019/2/25.
 * 最长公共子序列
 *
 * @author Zhoufy
 */
public class LongestCommonSubsequence {

    //优化
    public static int longest(String s1, String s2) {
        if (s1.length() == 0 || s2.length() == 0) {
            return 0;
        }
        char[] str1 = s1.toCharArray();
        char[] str2 = s2.toCharArray();
        int start1 = -1;
        int start2 = -1;
        int[][] results = new int[str2.length][str1.length];
        //最大长度
        int maxLength = 0;
        int compareNum = 0;
        for (int i = 0; i < str1.length; i++) {
            results[0][i] = (str2[0] == str1[i] ? 1 : 0);
            compareNum++;
            for (int j = 1; j < str2.length; j++) {
                results[j][0] = (str1[0] == str2[j] ? 1 : 0);
                if (i > 0 && j > 0) {
                    if (str1[i] == str2[j]) {
                        results[j][i] = results[j - 1][i - 1] + 1;
                        compareNum++;
                    }
                }
                if (maxLength < results[j][i]) {
                    maxLength = results[j][i];
                    start1 = i - maxLength + 2;
                    start2 = j - maxLength + 2;
                }
            }
        }
        System.out.println("比较次数" + (compareNum + str2.length) + "，s1起始位置：" + start1 + "，s2起始位置：" + start2);
        return maxLength;
    }

    public static int[][] getLength(String[] x, String[] y) {
        int[][] b = new int[x.length][y.length];
        int[][] c = new int[x.length][y.length];
        for (int i = 1; i < x.length; i++) {
            for (int j = 1; j < y.length; j++) {
                if (x[i] == y[j]) {
                    c[i][j] = c[i - 1][j - 1] + 1;
                    b[i][j] = 1;
                } else if (c[i - 1][j] >= c[i][j - 1]) {
                    c[i][j] = c[i - 1][j];
                    b[i][j] = 0;
                } else {
                    c[i][j] = c[i][j - 1];
                    b[i][j] = -1;
                }
            }
        }
        return b;
    }

    public static void Display(int[][] b, String[] x, int i, int j) {
        if (i == 0 || j == 0) {
            return;
        }
        if (b[i][j] == 1) {
            Display(b, x, i - 1, j - 1);
            System.out.print(x[i] + "");
        } else if (b[i][j] == 0) {
            Display(b, x, i - 1, j);
        } else if (b[i][j] == -1) {
            Display(b, x, i, j - 1);
        }
    }

    public static void main(String[] args) {

        String text = "a";


        System.out.println(text.substring(0, 0));

//        String s1 = "abcbdab";
//        String s2 = "bdcaba";
//
//        System.out.println(longest(s1, s2));
        String[] x = {"", "A", "B", "C", "B", "D", "A", "B"};
        String[] y = {"", "B", "D", "C", "A", "B", "A"};
//        int[][] b = getLength(x, y);
//        Display(b, x, x.length - 1, y.length - 1);
        String a = "abcbdab";
        String b = "bdcaba";

        int[][] len_vv = new int[10][10];
        int len1 = a.length();
        int len2 = b.length();

        for (int i = 1; i < len1 + 1; i++) {
            for (int j = 1; j < len2 + 1; j++) {
                if (a.substring(i - 1, i).equals(b.substring(j - 1, j))) {
                    len_vv[i][j] = 1 + len_vv[i - 1][j - 1];
                } else {
                    len_vv[i][j] = max(len_vv[i - 1][j], len_vv[i][j - 1]);
                }
            }
        }
        System.out.println(len_vv[len1][len2]);
    }
}
