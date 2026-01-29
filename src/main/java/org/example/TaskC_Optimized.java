package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskC_Optimized {

    // Mapper: skip header, emit Nationality -> 1 for each record
    public static class CountryCountMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final Text outKey = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            // skip header line (common header starts with "PersonID")
            if (line.startsWith("PersonID")) return;

            // split into up to 5 parts: PersonID,Name,Nationality,Country Code,Hobby
            String[] parts = line.split(",", 5);
            if (parts.length < 3) return;

            String nationality = parts[2].trim();
            if (!nationality.isEmpty()) {
                outKey.set(nationality);
                context.write(outKey, one);
            }
        }
    }

    // Reducer: sum counts per country (also used as combiner)
    public static class SumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String defaultInput = "src/data/pages.csv";
        String defaultOutput = "target/taskC-optimized-output";

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
        Job job = Job.getInstance(conf, "count facebook pages per country");
        // fix jar class reference
        job.setJarByClass(TaskC_Optimized.class);
        job.setMapperClass(CountryCountMapper.class);
        // use combiner to reduce network traffic
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        // explicitly set map output types (matches mapper output)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
