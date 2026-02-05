package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskB_Optimized {

    // 1. Mapper: each unique page gets 1
    public static class AccessMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pageID = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                pageID.set(parts[2].trim());
                context.write(pageID, one);
            }
        }
    }

    // 2. Combiner: to combine all mapped values
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // 3. Reducer: caches with pages, sums all of them together, then gets top 10
    public static class Top10Reducer extends Reducer<Text, IntWritable, Text, Text> {
        private Map<String, Integer> countMap = new HashMap<>();
        private Map<String, String> pageDetails = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader br = new BufferedReader(new FileReader("pages.csv"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split(",");
                        if (parts.length >= 3) {
                            pageDetails.put(parts[0].trim(), parts[1].trim() + ", " + parts[2].trim());
                        }
                    }
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Top 10 output
            List<Map.Entry<String, Integer>> list = new ArrayList<>(countMap.entrySet());
            list.sort((a, b) -> b.getValue().compareTo(a.getValue()));
            int limit = Math.min(10, list.size());
            for (int i = 0; i < limit; i++) {
                Map.Entry<String, Integer> entry = list.get(i);
                String id = entry.getKey();
                String info = pageDetails.getOrDefault(id, "Unknown");
                context.write(new Text(id + ", " + info), new Text("# of times accessed: "
                        + entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String defaultInput = "hdfs://localhost:9000/user/mahit/project1/access_logs.csv";
        String defaultOutput = "hdfs://localhost:9000/user/mahit/project1/taskB_optimzied_output";
        String inputPath;
        String outputPath;

        if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        } else if (args.length == 1) {
            inputPath = args[0];
            outputPath = defaultOutput;
            System.err.println("Using default paths:" + outputPath);
        } else {
            inputPath = defaultInput;
            outputPath = defaultOutput;
            System.err.println("Using default paths:");
        }
        System.err.println("Input: " + inputPath);
        System.err.println("Output: " + outputPath);

        Configuration conf = new Configuration();
        // needed to get file system to delete previous file cause old logic failed,
        Path out = new Path(outputPath);
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        Job job = Job.getInstance(conf, "Top 10 Pages");
        job.setJarByClass(TaskB_Optimized.class);
        job.setMapperClass(AccessMapper.class);
        //Utilized combiners here
        job.setCombinerClass(TaskB_Optimized.IntSumReducer.class);
        job.setReducerClass(TaskB_Optimized.Top10Reducer.class);
        job.setReducerClass(Top10Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // cache the file for the join
        job.addCacheFile(new URI("hdfs://localhost:9000/user/mahit/project1/pages.csv#pages.csv"));
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}