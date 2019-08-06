package com;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
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


    public static void main(String[] args) throws Exception {


        List<String> list = Files.readAllLines(Paths.get("/Users/zhoufy/Desktop/friend"));

        String line = list.get(0);
        String[] ss = line.split("\t");
        for (String s : ss){
            System.out.println(s);
        }
    }
}
