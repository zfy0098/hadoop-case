package com.leetcode;

/**
 * Created with IntelliJ IDEA by ChouFy on 2020-03-18.
 *
 * @author zhoufy
 */
public class IsRectangleOverlap {

    /**
     *
     *    矩形以列表 [x1, y1, x2, y2] 的形式表示，其中 (x1, y1) 为左下角的坐标，(x2, y2) 是右上角的坐标。
     *       如果相交的面积为正，则称两矩形重叠。需要明确的是，只在角或边接触的两个矩形不构成重叠。
     *       给出两个矩形，判断它们是否重叠并返回结果。
     *
     *    输入：rec1 = [0,1,2,3], rec2 = [1,2,3,4]
     *
     *               1 x 开头       x 结尾          1   y 开头      y 结尾
     *    rec1 x = 0[rec1[0]] - 2 [rec1[2]]  y = 1[rec1[1]] - 3[rec1[3]]
     *               2 x 开头       x 结尾          2   y 开头      y 结尾
     *    rec2 x = 1[rec2[0]] - 3 [rec2[2]]  y = 2[rec2[1]] - 4[rec2[3]]
     *
     *    1x 的结尾小于 2x 开头 不重叠
     *    2x 的结尾小于 1x 开头 不重叠
     *
     *    1y 的结尾小于 2y 开头 不重叠
     *    2y 的结尾小于 1y 开头 不重叠
     *
     *    boolean x = !(rec1[2] <= rec2[0] || rec1[2] <= rec2[2])
     *    boolean y = !(rec1[1] <= rec2[3] || rec1[3] <= rec2[1])
     *
     *
     *    输入：rec1 = [0,0,1,1], rec2 = [1,0,2,1]
     *    输出：false
     *
     *
     * @param rec1
     * @param rec2
     * @return
     */

    public static boolean isRectangleOverlap(int[] rec1, int[] rec2) {

        boolean x = !(rec1[2] <= rec2[0] || rec2[2] <= rec1[0]);
        boolean y = !(rec1[3] <= rec2[1] || rec2[3] <= rec1[1]);
        return x && y;
    }



    public static void main(String[] args) {
       int rec1[] = {0,1,2,3};
       int rec2[] = {2,3,3,4};
       boolean flag = isRectangleOverlap(rec1, rec2);
       System.out.println(flag);
    }
}
