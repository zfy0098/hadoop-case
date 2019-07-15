package com;

import java.io.File;

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


    public static void main(String[] args) {

        deleteDir("/Users/zhoufy/sparktest/demo10out");

//        File file = new File("/Users/zhoufy/sparktest/out10");
//        System.out.println(file.exists());
//        file.delete();
//        System.out.println(file.exists());

    }
}
