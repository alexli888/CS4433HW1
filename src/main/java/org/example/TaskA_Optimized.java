package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskA_Optimized {

    // Mapper: parse CSV, skip header, filter Nationality == "Italy", emit Name -> Hobby
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();
        private static final String TARGET_NATIONALITY = "Italy";

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Skip header
            if (line.startsWith("PersonID")) return;

            // PersonID,Name,Nationality,CountryCode,Hobby
            String[] parts = line.split(",", 5);
            if (parts.length < 5) return;

            String name = parts[1].trim();
            String nationality = parts[2].trim();
            String hobby = parts[4].trim();

            if (TARGET_NATIONALITY.equals(nationality)) {
                outKey.set(name);
                outVal.set(hobby);
                context.write(outKey, outVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String defaultInput = "src/data/pages.csv";
        String defaultOutput = "target/taskA-maponly-output";

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
            System.err.println("Using default input and output paths");
        }

        System.err.println("Input: " + inputPath);
        System.err.println("Output: " + outputPath);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "filter by nationality (map-only)");

        job.setJarByClass(TaskA.class);
        job.setMapperClass(TokenizerMapper.class);

        // MAP-ONLY JOB
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
