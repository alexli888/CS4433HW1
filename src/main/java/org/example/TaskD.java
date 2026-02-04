package org.example;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TaskD {

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
            // Friend table, p2
            else if (filename.contains("friends")) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    String myFriendID = parts[2].trim(); //getting the p2 for the join cause we are checking receiving
                    if (myFriendID.equals("MyFriend")) return;
                    outKey.set(myFriendID); //key to join on
                    outValue.set("FRIEND:1"); //for summing
                    context.write(outKey, outValue);
                }
            }
        }
    }

    // Left join and group by
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "Unknown";
            int friendCount = 0;

            //select
            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("NAME:")) {
                    name = strVal.substring(5);
                }
                else if (strVal.startsWith("FRIEND:")) {
                    friendCount++;
                }
            }
            context.write(new Text(key), new Text(name + "," + friendCount));
        }
    }

    public static void main(String[] args) throws Exception {
        String input1 = "hdfs://localhost:9000/user/mahit/project1/pages.csv";
        String input2 = "hdfs://localhost:9000/user/mahit/project1/friends.csv";
        String defaultOutput = "hdfs://localhost:9000/user/mahit/project1/taskD_output";

        String outputPath;

        if (args.length == 3) {
            input1 = args[0];
            input2 = args[1];
            outputPath = args[2];
        } else if (args.length == 1) {
            outputPath = args[0];
            System.err.println("Using default pathjs:" + outputPath);
        } else {
            outputPath = defaultOutput;
            System.err.println("Using default paths:");
        }
        System.err.println("Input 1: " + input1);
        System.err.println("Input 2: " + input2);
        System.err.println("Output: " + outputPath);

        Configuration conf = new Configuration();

        Path out = new Path(outputPath);
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        Job job = Job.getInstance(conf, "task D:");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}