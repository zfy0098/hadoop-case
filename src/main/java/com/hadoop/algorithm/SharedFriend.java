package com.hadoop.algorithm;

import com.utils.ParamsUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IDEA by ChouFy on 2019/8/13.
 *
 * @author Zhoufy
 */
public class SharedFriend {


    /**
     *    input text
     *     A:B,C,D,F,E,O
     *     B:A,C,E,K
     *     C:F,A,D,I
     *     D:A,E,F,L
     *     E:B,C,D,M,L
     *     F:A,B,C,D,E,O,M
     *     G:A,C,D,E,F
     *     H:A,C,D,E,O
     *     I:A,O
     *     J:B,O
     *     K:A,C,D
     *     L:D,E,F
     *     M:E,F,G
     *     O:A,H,I,J
     */

    /**
     *
     * map out :
     * friend : B , person : A
     * friend : C , person : A
     * friend : D , person : A
     * friend : F , person : A
     * friend : E , person : A
     * friend : O , person : A
     * friend : A , person : B
     * friend : C , person : B
     * friend : E , person : B
     * friend : K , person : B
     * friend : F , person : C
     * friend : A , person : C
     * friend : D , person : C
     * friend : I , person : C
     * friend : A , person : D
     * friend : E , person : D
     * friend : F , person : D
     * friend : L , person : D
     * friend : B , person : E
     * friend : C , person : E
     * friend : D , person : E
     * friend : M , person : E
     * friend : L , person : E
     * friend : A , person : F
     * friend : B , person : F
     * friend : C , person : F
     * friend : D , person : F
     * friend : E , person : F
     * friend : O , person : F
     * friend : M , person : F
     * friend : A , person : G
     * friend : C , person : G
     * friend : D , person : G
     * friend : E , person : G
     * friend : F , person : G
     * friend : A , person : H
     * friend : C , person : H
     * friend : D , person : H
     * friend : E , person : H
     * friend : O , person : H
     * friend : A , person : I
     * friend : O , person : I
     * friend : B , person : J
     * friend : O , person : J
     * friend : A , person : K
     * friend : C , person : K
     * friend : D , person : K
     * friend : D , person : L
     * friend : E , person : L
     * friend : F , person : L
     * friend : E , person : M
     * friend : F , person : M
     * friend : G , person : M
     * friend : A , person : O
     * friend : H , person : O
     * friend : I , person : O
     * friend : J , person : O
     *
     *
     * 第一阶段的map函数主要完成以下任务
     *      * 1.遍历原始文件中每行<所有朋友>信息
     *      * 2.遍历“朋友”集合，以每个“朋友”为键，原来的“人”为值  即输出<朋友,人>
     */
    public static class SharedFriendMapperStepOne extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] personFriends = line.split(":");
            String person = personFriends[0];
            String[] friends = personFriends[1].split(",");

            for (String friend : friends) {
                System.out.println("friend : " + friend + " , person : " + person);
                context.write(new Text(friend), new Text(person));
            }

        }
    }


    /**
     * 第一阶段的reduce函数主要完成以下任务
     * 1.对所有传过来的<朋友，list(人)>进行拼接，输出<朋友,拥有这名朋友的所有人>
     * <p>
     * <p>
     * <p>
     * A	I,K,C,B,G,F,H,O,D
     * B	A,F,J,E
     * C	A,E,B,H,F,G,K
     * D	G,C,K,A,L,F,E,H
     * E	G,M,L,H,A,F,B,D
     * F	L,M,D,C,G,A
     * G	M
     * H	O
     * I	O,C
     * J	O
     * K	B
     * L	D,E
     * M	E,F
     * O	A,H,I,J,F
     */
    public static class SharedFriendReducerStepOne extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text text : values) {
                sb.append(text.toString()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            context.write(key, new Text(sb.toString()));
        }
    }


    /**
     * 第二阶段的map函数主要完成以下任务
     * 1.将上一阶段reduce输出的<朋友,拥有这名朋友的所有人>信息中的 “拥有这名朋友的所有人”进行排序 ，以防出现B-C C-B这样的重复
     * 2.将 “拥有这名朋友的所有人”进行两两配对，并将配对后的字符串当做键，“朋友”当做值输出，即输出<人-人，共同朋友>
     *    map out
     *    A-B:	E
     *    A-B:	C
     *    A-C:	D
     *    A-C:	F
     *    A-D:	E
     *    A-D:	F
     *    A-E:	D
     *    A-E:	B
     *    A-E:	C
     *    A-F:	O
     *    A-F:	B
     *    A-F:	C
     *    A-F:	D
     *    A-F:	E
     *    A-G:	F
     *    A-G:	E
     *    A-G:	C
     *    A-G:	D
     *    A-H:	E
     *    A-H:	C
     *    A-H:	D ....
     */
    public static class SharedFriendMapStepTwo extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] friendPersons = line.split("\t");
            String friend = friendPersons[0];

            String[] persons = friendPersons[1].split(",");
            //排序
            Arrays.sort(persons);

            System.out.println("friend : " + friend + " , " + Arrays.toString(persons));
            //两两配对
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    context.write(new Text(persons[i] + "-" + persons[j] + ":"), new Text(friend));
                }
            }
        }
    }


    /**
     * 第二阶段的reduce函数主要完成以下任务
     * 1.<人-人，list(共同朋友)> 中的“共同好友”进行拼接 最后输出<人-人，两人的所有共同好友>
     */
    public static class SharedFriendReducerStepTwo extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();
            Set<String> set = new HashSet<>();
            for (Text friend : values) {
                if (!set.contains(friend.toString())){
                    set.add(friend.toString());
                }
            }
            for (String friend : set) {
                sb.append(friend).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {


        args = new String[]{"hdfs://master:9000/friend/friendText", "hdfs://master:9000/friend/SharedFriendOut"};

        Job job = ParamsUtils.pretreatment(args, 2);

        job.setJobName("SharedFriend");
        job.setJarByClass(SharedFriend.class);

        job.setMapperClass(SharedFriendMapperStepOne.class);
        job.setReducerClass(SharedFriendReducerStepOne.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);


        args = new String[]{"hdfs://master:9000/friend/SharedFriendOut/part-r-00000", "hdfs://master:9000/friend/SharedFriendOutStepTwo"};
        Job job1 = ParamsUtils.pretreatment(args, 2);
        job1.setJobName("SharedFriendTwo");
        job1.setJarByClass(SharedFriend.class);

        job1.setMapperClass(SharedFriendMapStepTwo.class);
        job1.setReducerClass(SharedFriendReducerStepTwo.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.waitForCompletion(true);
    }
}
