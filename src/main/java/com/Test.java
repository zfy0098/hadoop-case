package com;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IDEA by ChouFy on 2019/5/30.
 *
 * @author Zhoufy
 */
public class Test {
    public static void deleteDir(String dirPath) {
        File file = new File(dirPath);
        if (file.isFile()) {
            file.delete();
        } else {
            File[] files = file.listFiles();
            if (files == null) {
                file.delete();
            } else {
                for (int i = 0; i < files.length; i++) {
                    deleteDir(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }

    }




    public static void test(int[] nums, int num){

        boolean flag = true;
        int[] files = new int[nums.length + 1];
        for (int i = 0; i < nums.length; i++){
            int x = nums[i];
            if(x > num){
                if(flag){
                    files[i] = num;
                    flag = false;
                }
                files[i + 1] = x;

            } else {
                files[i] = x;
            }
        }


        System.out.println(Arrays.toString(files));


    }



    public static void main(String[] args) throws Exception {







    }
}


