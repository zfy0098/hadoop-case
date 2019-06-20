package com.hadoop.leftouterjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;


/**
 * Created with IDEA by ChouFy on 2019/6/19.
 *
 * @author Zhoufy
 */
public class Case {


    public static class MapOutWritable implements WritableComparable<MapOutWritable> {
        private String musicID;
        private String userID;
        private String musicName;
        private String watchLen;
        private String hour;

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(musicID);
            dataOutput.writeUTF(musicName);
            dataOutput.writeUTF(userID);
            dataOutput.writeUTF(watchLen);
            dataOutput.writeUTF(hour);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.musicID = dataInput.readUTF();
            this.musicName = dataInput.readUTF();
            this.userID = dataInput.readUTF();
            this.watchLen = dataInput.readUTF();
            this.hour = dataInput.readUTF();
        }


        @Override
        public int compareTo(MapOutWritable o) {
            int compare = 0;
            compare = musicID.compareTo(o.musicID);
            if (compare != 0) {
                return compare;
            }
            compare = userID.compareTo(o.userID);
            if (compare != 0) {
                return compare;
            }
            return compare;
        }


        public String getMusicID() {
            return musicID;
        }

        public void setMusicID(String musicID) {
            this.musicID = musicID;
        }

        public String getUserID() {
            return userID;
        }

        public void setUserID(String userID) {
            this.userID = userID;
        }

        public String getMusicName() {
            return musicName;
        }

        public void setMusicName(String musicName) {
            this.musicName = musicName;
        }

        public String getWatchLen() {
            return watchLen;
        }

        public void setWatchLen(String watchLen) {
            this.watchLen = watchLen;
        }

        public String getHour() {
            return hour;
        }

        public void setHour(String hour) {
            this.hour = hour;
        }


        @Override
        public String toString() {
            return musicID + '\t' + userID + '\t' + musicName + '\t' + watchLen + '\t' + hour;
        }
    }


    public static class MusicMapper extends Mapper<LongWritable, Text, Text, MapOutWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            MapOutWritable mapOutWritable = new MapOutWritable();
            String[] texts = line.split("\001");
            if (texts.length > 1) {
                mapOutWritable.setMusicID(texts[0]);
                mapOutWritable.setMusicName(texts[1]);
                mapOutWritable.setHour("");
                mapOutWritable.setUserID("");
                mapOutWritable.setWatchLen("");
                context.write(new Text(texts[0]), mapOutWritable);
            }
        }
    }


    public static class WatchMapper extends Mapper<LongWritable, Text, Text, MapOutWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] datas = line.split("\001");
            if (datas.length == 4) {
                MapOutWritable mapOutWritable = new MapOutWritable();
                mapOutWritable.setUserID(datas[0]);
                mapOutWritable.setMusicID(datas[1]);
                mapOutWritable.setWatchLen(datas[2]);
                mapOutWritable.setHour(datas[3]);
                mapOutWritable.setMusicName("");
                context.write(new Text(datas[1]), mapOutWritable);
            }
        }
    }

    public static class MusicWatchReducer extends Reducer<Text, MapOutWritable , NullWritable, Text>{
        @Override
        protected void reduce(Text key, Iterable<MapOutWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<MapOutWritable> iterable = values.iterator();

            String musicName = "";
            if(iterable.hasNext()){
                musicName = iterable.next().getMusicName();
            }

            while (iterable.hasNext()){
                MapOutWritable mapOutWritable = iterable.next();
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(mapOutWritable.getMusicID());
                stringBuilder.append("\t");
                stringBuilder.append(musicName);
                stringBuilder.append("\t");
                stringBuilder.append(mapOutWritable.getUserID());
                stringBuilder.append("\t");
                stringBuilder.append(mapOutWritable.getHour());
                stringBuilder.append("\t");
                stringBuilder.append(mapOutWritable.getWatchLen());
                context.write(NullWritable.get(), new Text(stringBuilder.toString()));
            }
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            return;
        }

        Path music = new Path(args[0]);
        Path watch = new Path(args[1]);
        Path out = new Path(args[2]);

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.deleteOnExit(out);
        }
        fs.close();


        Job job = Job.getInstance(conf, "case");

        job.setJarByClass(Case.class);

        job.setReducerClass(MusicWatchReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapOutWritable.class);

        MultipleInputs.addInputPath(job, music, TextInputFormat.class, MusicMapper.class);
        MultipleInputs.addInputPath(job, watch, TextInputFormat.class, WatchMapper.class);

        FileOutputFormat.setOutputPath(job, out);

        job.waitForCompletion(true);

    }
}
