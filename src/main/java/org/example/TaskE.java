package org.example;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 TaskE: For each person (ByWho) compute:
   - total accesses (count of log lines for that person)
   - distinct pages accessed (unique WhatPage values)

 Input format (AccessLog): AccessId,ByWho,WhatPage,TypeOfAccess,AccessTime
 The mapper emits: key = ByWho, value = WhatPage
 The reducer counts total values and unique pages using a HashSet.
*/
public class TaskE {

    public static class CountryAccessMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // split into up to 5 parts so TypeOfAccess may contain commas
            String[] parts = line.split(",", 5);
            if (parts.length < 3) return;

            // Robustly skip header or malformed lines: ensure AccessId is numeric
            String accessIdStr = parts[0].trim();
            try {
                Integer.parseInt(accessIdStr);
            } catch (NumberFormatException e) {
                return; // not a data line (likely header)
            }

            String byWho = parts[1].trim();
            String whatPage = parts[2].trim();

            if (!byWho.isEmpty() && !whatPage.isEmpty()) {
                outKey.set(byWho);
                outVal.set(whatPage);
                context.write(outKey, outVal);
            }
        }
    }

    public static class UserAccessReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            HashSet<String> distinctPages = new HashSet<>();

            for (Text v : values) {
                total++;
                distinctPages.add(v.toString());
            }

            // value format: totalAccesses \t distinctPagesCount
            outVal.set(total + "\t" + distinctPages.size());
            context.write(key, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        String defaultInput = "src/data/access_logs.csv";
        String defaultOutput = "target/taskE-output";

        String inputPath;
        String outputPath;

        if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        } else if (args.length == 1) {
            inputPath = args[0];
            outputPath = defaultOutput;
            System.err.println("Using default output path: " + outputPath);
        } else {
            inputPath = defaultInput;
            outputPath = defaultOutput;
            System.err.println("Using default paths:");
        }
        System.err.println("Input: " + inputPath);
        System.err.println("Output: " + outputPath);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "total and distinct facebook pages per user");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(CountryAccessMapper.class);
        job.setReducerClass(UserAccessReducer.class);
        // explicit map output classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
