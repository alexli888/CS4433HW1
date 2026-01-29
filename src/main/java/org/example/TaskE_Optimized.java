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
public class TaskE_Optimized {

    // Mapper
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

            // skip header or malformed lines
            String accessIdStr = parts[0].trim();
            try {
                Integer.parseInt(accessIdStr);
            } catch (NumberFormatException e) {
                return; // not a data line
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

    // Reducer (also used as Combiner)
    public static class UserAccessReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            HashSet<String> distinctPages = new HashSet<>();

            for (Text v : values) {
                String val = v.toString();

                // handle normal mapper output
                if (!val.contains("|")) {
                    total++;
                    distinctPages.add(val);
                } else {
                    // handle combiner/reducer intermediate output: count|page1|page2|...
                    String[] parts = val.split("\\|");
                    total += Integer.parseInt(parts[0]);
                    for (int i = 1; i < parts.length; i++) {
                        distinctPages.add(parts[i]);
                    }
                }
            }

            // emit in same format as mapper for combiner
            StringBuilder sb = new StringBuilder();
            sb.append(total);
            for (String page : distinctPages) {
                sb.append("|").append(page);
            }
            outVal.set(sb.toString());
            context.write(key, outVal);
        }
    }

    // Final Reducer to output only total and distinct counts
    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            HashSet<String> distinctPages = new HashSet<>();

            for (Text v : values) {
                String val = v.toString();
                String[] parts = val.split("\\|");
                total += Integer.parseInt(parts[0]);
                for (int i = 1; i < parts.length; i++) {
                    distinctPages.add(parts[i]);
                }
            }

            outVal.set(total + "\t" + distinctPages.size());
            context.write(key, outVal);
        }
    }

    // Main
    public static void main(String[] args) throws Exception {
        String defaultInput = "src/data/access_logs.csv";
        String defaultOutput = "target/taskE-optimized-output";

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
        job.setJarByClass(TaskE_Optimized.class);
        job.setMapperClass(CountryAccessMapper.class);
        job.setCombinerClass(UserAccessReducer.class);  // COMBINER
        job.setReducerClass(FinalReducer.class);        // FINAL REDUCER

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
