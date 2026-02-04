package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class TaskD_Optimized {

    public static class JoinMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            //need this so we know which file we are currently reading
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            // my page
            if (filename.contains("pages")) {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    String personID = parts[0].trim();
                    String personName = parts[1].trim();
                    if (personID.equals("ID")) return;
                    outKey.set(personID);
                    outValue.set("NAME:" + personName);
                    context.write(outKey, outValue);
                }
            }
            // friends table, p2
            else if (filename.contains("friends")) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    String myFriendID = parts[2].trim();
                    if (myFriendID.equals("MyFriend")) return;
                    outKey.set(myFriendID);
                    //the 1 for the combiner now
                    outValue.set("FRIEND:1");
                    context.write(outKey, outValue);
                }
            }
        }
    }

    // This is the combiner for 1s
    public static class FriendCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int localSum = 0;

            //select w combiner
            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("NAME:")) {
                    context.write(key, val);
                }
                else if (strVal.startsWith("FRIEND:")) {
                    localSum += Integer.parseInt(strVal.split(":")[1]);
                }
            }
            // the total count
            if (localSum > 0) {
                context.write(key, new Text("FRIEND:" + localSum));
            }
        }
    }

    // *** UPDATED REDUCER ***
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "Unknown";
            int totalFriendCount = 0;

            //select
            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("NAME:")) {
                    name = strVal.substring(5);
                }
                else if (strVal.startsWith("FRIEND:")) {
                   //parsing the friend out so its not just a ++ like in normal D
                    totalFriendCount += Integer.parseInt(strVal.split(":")[1]);
                }
            }
            context.write(new Text(key), new Text(name + "," + totalFriendCount));
        }
    }

    public static void main(String[] args) throws Exception {
        String input1 = "hdfs://localhost:9000/user/mahit/project1/pages.csv";
        String input2 = "hdfs://localhost:9000/user/mahit/project1/friends.csv";
        String defaultOutput = "hdfs://localhost:9000/user/mahit/project1/taskD_optimized_output";

        if (args.length == 3) {
            input1 = args[0]; input2 = args[1]; defaultOutput = args[2];
        }

        Configuration conf = new Configuration();
        Path out = new Path(defaultOutput);
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) fs.delete(out, true);

        Job job = Job.getInstance(conf, "Task D Optimized");
        job.setJarByClass(TaskD_Optimized.class);
        job.setMapperClass(JoinMapper.class);
        //using hte combiner now
        job.setCombinerClass(FriendCombiner.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(defaultOutput));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}