package com.hadoop;

/**
 * Created with IDEA by ChouFy on 2019/6/5.
 *
 * @author Zhoufy
 */
import java.io.*;
/**
 * 本类是演示使用apktool在动态的解压，打包，签名一个apk，用来向apk写入一些信息，比如推广的时候
 * 使用本类需要在执行的机器上面配置java，apktool。
 * Author:Mr.Jie
 * DateTime:16-11-6 上午11:02
 */
public class Main {
    public static void main(String[] args) {

        Process process = null;
        //记录处理开始的时间
        long start = System.currentTimeMillis();

        //为了方便使用我把所有需要配置的信息，全部写在下面了（所有配置均不能包含空格）

        //java的位置,如果你有环境变量，可以直接写java
        String javaPath="java";
        //apktool路径
        String apktoolPath = "apktool_2.2.1.jar";
        //需要解压的apk文件路径
        String apkPath = "weixin.apk";
        //保存解压以后文件夹的路径
        String unPackagePath = "myapp/";
        //重新打包以后，apk存放的路径
        String rePackagePath = "repackage.apk";
        //签名以后apk的
        String signedApkPath = "signedApk.apk";
        //jarsigner的路径，一般在jdk的bin目录，用于对apk签名
        String jarsignerPath = "jarsigner";
        //用于签名的密钥库的路径
        String keyPath = "testkey.jks";
        //密钥库完整性的口令
        String keyStorepass = "123456";
        //使用密钥库里面密钥的别名
        String keyAlias = "测试签名";
        //密钥的口令
        String keypass = "123456";
        //1----解压apk
        try {
            //解压apk文件包
            String unPackageCmd = javaPath+" -jar " + apktoolPath + " d -f -o " + unPackagePath + " " + apkPath;
            System.out.println("1.正在执行解压:" + unPackageCmd);
            process = Runtime.getRuntime().exec(unPackageCmd);
            if (process.waitFor() != 0) {
                System.out.println("解压失败，过程终止");
                return;
            }
            System.out.println("解压成功");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //2----内容修改，这里是在assets文件夹里面写入信息
        OutputStreamWriter osw = null;
        try {
            String targetFilePath = unPackagePath + "assets";
            File wfile = new File(targetFilePath);
            if (!wfile.exists()) {
                wfile.mkdirs();
            }
            wfile = new File(wfile, "ad.conf");
            String content = "这里是解压以后，新写入的内容";
            System.out.println("2.正在执行写入：" + content);
            osw = new OutputStreamWriter(new FileOutputStream(wfile));
            osw.write(content, 0, content.length());
            osw.flush();
            System.out.println("写入文件新内容成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("写入文件信息出错，过程终止");
            return;
        } finally {
            try {
                osw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }


        //3----重新打包
        try {
            String packageCmd =javaPath+" -jar " + apktoolPath + " b -o  " + rePackagePath + " " + unPackagePath;
            System.out.println("3.正在重新打包apk：" + packageCmd);
            process = Runtime.getRuntime().exec(packageCmd);
            if (process.waitFor() != 0) {
                System.out.println("打包失败，过程终止");
                return;
            }
            System.out.println("打包成功");
        } catch (Exception e) {
            e.printStackTrace();
        }


        //4----对apk签名
        try {
            String signCmd = jarsignerPath + " -keystore " + keyPath + " -storepass " + keyStorepass + " -signedjar " + signedApkPath + " " + rePackagePath + " -keypass " + keypass + " " + keyAlias;
            System.out.println("4.正在签名apk：" + signCmd);
            process = Runtime.getRuntime().exec(signCmd);
            if (process.waitFor() != 0) {
                System.out.println("签名失败。。。");
                return;
            }
            System.out.println("签名成功");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //5----删除临时文件
        try {
            System.out.println("5.正在删除临时文件");
            //删除生成的没有前面的apk
            delDir(rePackagePath);
            //删除解压的目录
            delDir(unPackagePath);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("删除临时文件出错："+e.getMessage());

        }

        float end = (System.currentTimeMillis() - start) / 1000f;
        System.out.println("解压，打包，签名apk完成，一共耗时:" + end + "S");

    }

    /**
     * 用来删除一个文件夹或者文件（写的有点麻烦，主要是删除文件夹，如果在Liunx下面，可以直接勇敢的调用rm命令，为了兼容windows，我就直接用java实现了）
     * @param filePath 文件夹路径
     */
    private static  void delDir(String filePath) {
        File file = new File(filePath);
        //不存在就返回
        if (!file.exists()) return;

        if (file.isFile()) {
            //是文件就直接删除
            file.delete();
            return;
        } else {
            //是文件夹就需要迭代的删除
            File[] files = file.listFiles();
            int len = files.length;
            for (int i = 0; i < len; i++) {
                if (files[i].isFile()) {
                    //是文件就直接删除
                    files[i].delete();
                } else {
                    //是文件夹就递归调用
                    delDir(files[i].getPath());
                }
            }
            file.delete();
        }
    }
}
