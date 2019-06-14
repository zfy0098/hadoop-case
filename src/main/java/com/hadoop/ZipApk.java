package com.hadoop;

import java.io.*;


/**
 * Created with IDEA by ChouFy on 2019/6/5.
 *
 * @author Zhoufy
 */
public class ZipApk {


    public static void main(String[] args) {
        //记录处理开始的时间
        long start = System.currentTimeMillis();

        //为了方便使用我把所有需要配置的信息，全部写在下面了（所有配置均不能包含空格）

        //java的位置,如果你有环境变量，可以直接写java
        //需要解压的apk文件路径
        String apkPath = "hlzg_1.0.8_bilibili_3049.apk";
        //保存解压以后文件夹的路径
        String unPackagePath = "myapp";
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
            String unPackageCmd = "unzip -o " + apkPath + " -d  " + unPackagePath;
            System.out.println("1.正在执行解压:" + unPackageCmd);
            Process process = Runtime.getRuntime().exec(unPackageCmd);

            new Thread(new Runnable() {

                @Override
                public void run() {
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(process.getInputStream()));
                    try {
                        String line;
                        while ((line = br.readLine()) != null) {
                            System.out.print(line);
                        }
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
            BufferedReader br = null;
            br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
            br.close();
            process.destroy();


            if (process.waitFor() != 0) {
                System.out.println(in2String(process.getInputStream()));
                System.out.println("解压失败，过程终止");
                return;
            }
            System.out.println("解压成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        //2----写入信息
        OutputStreamWriter osw = null;
        try {
            String targetFilePath = unPackagePath + "/assets";
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

        //3----删除原来的签名，不然最后打包出来的应用安装的时候，会提示签名错误
        String s = unPackagePath + "META-INF";
        System.out.println("3.删除原来的签名等信息：" + s);
        delDir(s);
        System.out.println("删除成功");


        //4----重新打包
        try {
            String packageCmd = "zip -r  " + rePackagePath + "  .";
            System.out.println("3.正在重新打包apk：" + packageCmd);
            Process process = Runtime.getRuntime().exec(packageCmd, null, new File(unPackagePath));

            new Thread(new Runnable() {

                @Override
                public void run() {
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(process.getInputStream()));
                    try {
                        String line;
                        while ((line = br.readLine()) != null) {
                            System.out.print(line);
                        }
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
            BufferedReader br = null;
            br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
            br.close();
            process.destroy();

            if (process.waitFor() != 0) {
                System.out.println(in2String(process.getInputStream()));
                System.out.println("打包失败，过程终止");
                return;
            }
            System.out.println("打包成功");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //5----打包以后目录不对，移动到指定的位置
        try {
            String moveCmd = "mv " + rePackagePath + "  ..";
            System.out.println("3.正在移动重新打包apk：" + moveCmd);
            Process process = Runtime.getRuntime().exec(moveCmd, null, new File(unPackagePath));

            new Thread(new Runnable() {
                @Override
                public void run() {
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(process.getInputStream()));
                    try {
                        while (br.readLine() != null) {

                        }
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
            BufferedReader br = null;
            br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
            br.close();
            process.destroy();

            if (process.waitFor() != 0) {
                System.out.println(in2String(process.getInputStream()));
                System.out.println("移动失败，过程终止");
                return;
            }
            System.out.println("移动成功");
        } catch (Exception e) {
            e.printStackTrace();
        }


        //6----对apk签名
        try {
            String signCmd = jarsignerPath + " -keystore " + keyPath + " -storepass " + keyStorepass + " -signedjar " + signedApkPath + " " + rePackagePath + " -keypass " + keypass + " " + keyAlias;
            System.out.println("4.正在签名apk：" + signCmd);
            Process process = Runtime.getRuntime().exec(signCmd);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(process.getInputStream()));
                    try {
                        while (br.readLine() != null) {

                        }
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
            BufferedReader br;
            br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
            br.close();
            process.destroy();

            if (process.waitFor() != 0) {
                System.out.println(in2String(process.getInputStream()));
                System.out.println("签名失败。。。");
                return;
            }
            System.out.println("签名成功");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //7----删除临时文件
        try {
            System.out.println("5.正在删除临时文件");
            //删除生成的没有签名的apk
            delDir(rePackagePath);
            //删除解压的目录
            delDir(unPackagePath);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("删除临时文件出错：" + e.getMessage());
        }

        float end = (System.currentTimeMillis() - start) / 1000f;
        System.out.println("解压，打包，签名apk完成，一共耗时:" + end + "S");


    }

    //转换成字符串的函数，用来读取出错信息
    public static String in2String(InputStream in) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        byte[] b = new byte[1024];
        int len;
        try {
            while ((len = in.read(b)) != -1) {
                byteOut.write(b, 0, len);
            }
            return byteOut.toString("UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 用来删除一个文件夹或者文件,用来删除临时文件
     *
     * @param filePath 文件夹路径
     */
    private static void delDir(String filePath) {
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
