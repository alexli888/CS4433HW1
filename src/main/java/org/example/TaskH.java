package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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

public class TaskH {
    //global variables
    public static enum Stats {
        TOTAL_FRIENDS,
        TOTAL_PEOPLE
    }

    //gets a count, 1 for the person and the friend in every row
    public static class FriendsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text id = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts[0].startsWith("FriendRel") || parts[1].equals("PersonID")) return;

            if (parts.length >= 3) {
                String p1 = parts[1].trim();
                String p2 = parts[2].trim();
                id.set(p1); context.write(id, one);
                id.set(p2); context.write(id, one);
            }
        }
    }
    //sums the 1 to calculate total friends per person
    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) { sum += val.get(); }
            result.set(sum);
            context.write(key, result);
            context.getCounter(Stats.TOTAL_FRIENDS).increment(sum); //update globals
            context.getCounter(Stats.TOTAL_PEOPLE).increment(1);
        }
    }
    //filters for friends count > average
    public static class PopularMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, String> nameMap = new HashMap<>();
        private double average = 0.0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            average = conf.getDouble("avgFriendCount", 0.0);

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader br = new BufferedReader(new FileReader("pages.csv"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.startsWith("PersonID")) continue;
                        String[] parts = line.split(",");
                        if (parts.length >= 2) {
                            nameMap.put(parts[0].trim(), parts[1].trim());
                        }
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\\t"); // Job 1 output is tab separated
            if (parts.length >= 2) {
                String personID = parts[0];
                int count = Integer.parseInt(parts[1]);

                if (count > average) {
                    String name = nameMap.getOrDefault(personID, "Unknown");
                    context.write(new Text(name), new Text("(total friends: " + count + ")"));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String input1 = "hdfs://localhost:9000/user/mahit/project1/pages.csv";
        String input2 = "hdfs://localhost:9000/user/mahit/project1/friends.csv";
        String tempOutput = "hdfs://localhost:9000/user/mahit/project1/taskH_temp";
        String defaultOutput = "hdfs://localhost:9000/user/mahit/project1/taskH_output";
        String outputPath;

        if (args.length == 3) {
            input1 = args[0];
            input2 = args[1];
            outputPath = args[2];
        } else if (args.length == 1) {
            outputPath = args[0];
            System.err.println("Using default paths:" + outputPath);
        } else {
            outputPath = defaultOutput;
            System.err.println("Using default paths:");
        }

        System.err.println("Input 1 (Pages): " + input1);
        System.err.println("Input 2 (Friends): " + input2);
        System.err.println("Output: " + outputPath);

        Configuration conf = new Configuration();
        Path out = new Path(outputPath);
        Path temp = new Path(tempOutput);
        FileSystem fs = out.getFileSystem(conf);

        if (fs.exists(out))
        { fs.delete(out, true); }
        if (fs.exists(temp))
        { fs.delete(temp, true); }
        //first query
        Job job1 = Job.getInstance(conf, "Task H 1:");
        job1.setJarByClass(TaskH.class);
        job1.setMapperClass(FriendsMapper.class);
        job1.setReducerClass(CountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(input2));
        FileOutputFormat.setOutputPath(job1, temp); // Write to temp
        //got from gemini for debugging
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        //get the avg
        long totalFriends = job1.getCounters().findCounter(Stats.TOTAL_FRIENDS).getValue();
        long totalPeople = job1.getCounters().findCounter(Stats.TOTAL_PEOPLE).getValue();
        double avg = (double) totalFriends / totalPeople;

        // query 2 for popularity
        Configuration conf2 = new Configuration();
        conf2.setDouble("avgFriendCount", avg);

        Job job2 = Job.getInstance(conf2, "Task H 2:");
        job2.setJarByClass(TaskH.class);
        job2.setMapperClass(PopularMapper.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, temp);
        FileOutputFormat.setOutputPath(job2, out);

        job2.addCacheFile(new URI(input1 + "#pages.csv"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}