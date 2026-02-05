package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//SQL QUERY
//Had Gemini help with query

//my original query
//SELECT DISTINCT
// pages.ID
// pages.Name
//FROM
// Friends
//LEFT JOIN
//  pages ON Friends.PersonID = pages.ID
//LEFT JOIN
//  Struggled here
//WHERE
//  access.ID is null



//SELECT DISTINCT
//  M.ID,
//  M.Name
//FROM
//  Friends F
//JOIN
//  MyPage M ON F.PersonID = M.ID    -- Get p1's Name
//LEFT JOIN
//  AccessLog A ON F.PersonID = A.ByWho AND F.MyFriend = A.WhatPage
//WHERE
//  A.AccessID IS NULL;
public class TaskF {

    public static class JoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) return;
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String[] parts = line.split(",");

            // friends csv
            if (filename.contains("friends")) {
                //skip header
                if (parts[0].startsWith("FriendRel") || parts[1].equals("PersonID")) return;
                if (parts.length >= 3) {
                    String p1 = parts[1].trim();
                    String p2 = parts[2].trim();
                    outKey.set(p1 + "," + p2);
                    outValue.set("REL:FRIEND");
                    context.write(outKey, outValue);
                }
            }
            // AccessLogs
            else if (filename.contains("access_logs")) {
                //skip
                if (parts[0].startsWith("AccessID") || parts[1].equals("ByWho")) return;
                if (parts.length >= 3) {
                    String byWho = parts[1].trim();
                    String whatPage = parts[2].trim();
                    outKey.set(byWho + "," + whatPage);
                    outValue.set("REL:ACCESS");
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class NegativeJoinReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> nameMap = new HashMap<>();
        private Set<String> alreadyReported = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // my page csv from cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader br = new BufferedReader(new FileReader("pages.csv"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        //skip
                        if (line.startsWith("PersonID")) continue;
                        String[] parts = line.split(",");
                        if (parts.length >= 2) {
                            nameMap.put(parts[0].trim(), parts[1].trim());
                        }
                    }
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isFriend = false;
            boolean hasAccessed = false;

            for (Text val : values) {
                String tag = val.toString();
                if (tag.equals("REL:FRIEND")) {
                    isFriend = true;
                } else if (tag.equals("REL:ACCESS")) {
                    hasAccessed = true;
                }
            }
            //if friend is true and has accessed is false.
            if (isFriend && !hasAccessed) {
                String p1ID = key.toString().split(",")[0];

                // Needed gemini for this logic cause I kept getting duplicates, didnt really know
                // how to filter out the people
                if (!alreadyReported.contains(p1ID)) {
                    String p1Name = nameMap.getOrDefault(p1ID, "null");
                    context.write(new Text(p1ID), new Text(p1Name));
                    alreadyReported.add(p1ID);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // friends and access log
        String input1 = "hdfs://localhost:9000/user/mahit/project1/friends.csv";
        String input2 = "hdfs://localhost:9000/user/mahit/project1/access_logs.csv";
        String pages = "hdfs://localhost:9000/user/mahit/project1/pages.csv";

        String defaultOutput = "hdfs://localhost:9000/user/mahit/project1/taskF_output";
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

        // delete for rerun
        Path out = new Path(outputPath);
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        Job job = Job.getInstance(conf, "task F:");
        job.setJarByClass(TaskF.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(NegativeJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        //my page cache
        job.addCacheFile(new URI(pages + "#pages.csv"));

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
