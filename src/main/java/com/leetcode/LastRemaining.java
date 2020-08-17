package com.leetcode;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA by ChouFy on 2020-03-30.
 * <p>
 * f(N,M)=(f(N−1,M)+M)%N
 *
 * @author zhoufy
 */
public class LastRemaining {


    public int lastRemaining(int n, int m) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(i);
        }
        int j = 0;
        while (list.size() > 1) {
            j = (j + m - 1) % n;
            list.remove(j);
            n--;
        }
        return 0;
    }


    public int nextRemaining3(int n, int m) {
        int p = 0;
        for (int i = 2; i <= n; i++) {
            p = (p + m) % i;
        }
        return p + 1;
    }

    public static void main(String[] args) {

        LastRemaining lastRemaining = new LastRemaining();


        lastRemaining.lastRemaining(5, 3);
    }
}
