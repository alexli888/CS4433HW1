package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskG {

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text ACTIVE = new Text("ACTIVE");
        private long thresholdMillis;
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        protected void setup(Context context) {
            thresholdMillis = System.currentTimeMillis() - 20L * 365 * 24 * 60 * 60 * 1000; // last 20 years

            System.out.println("Mapper setup: thresholdMillis = " + thresholdMillis);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.trim().isEmpty()) return;
            String low = line.toLowerCase();
            if (low.contains("accessid") && low.contains("bywho")) return; // skip header

            String[] parts = splitCsvLine(line);
            if (parts.length < 5) return;
            String byWho = parts[1].trim();
            String accessTimeStr = parts[4].trim();
            if (byWho.isEmpty() || accessTimeStr.isEmpty()) return;

            try {
                Date d = sdf.parse(accessTimeStr);
                if (d.getTime() >= thresholdMillis) {
                    context.write(new Text(byWho), ACTIVE);
                    System.out.println("Mapper emitting ACTIVE for PersonID: " + byWho + " at " + accessTimeStr);
                }
            } catch (ParseException e) {
                System.out.println("Mapper parse error for line: " + line);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private final Map<String, String> pages = new HashMap<>();
        private final Set<String> activeIds = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                String name = new Path(uri.getPath()).getName();
                try (BufferedReader br = new BufferedReader(new FileReader(name))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;
                        if (line.toLowerCase().startsWith("personid,")) continue; // skip header
                        String[] parts = splitCsvLine(line);
                        if (parts.length < 2) continue;
                        String id = parts[0].trim();
                        String nameCol = parts[1].trim();
                        pages.put(id, nameCol);
                    }
                }
            }
            System.out.println("Reducer setup: loaded pages.csv with " + pages.size() + " entries");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            for (Text v : values) {
                if (v != null && "ACTIVE".equals(v.toString())) {
                    activeIds.add(key.toString());
                    System.out.println("Reducer marking ACTIVE PersonID: " + key.toString());
                    break;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text outKey = new Text();
            Text outVal = new Text();
            int count = 0;
            for (Map.Entry<String, String> e : pages.entrySet()) {
                String id = e.getKey();
                if (!activeIds.contains(id)) {
                    outKey.set(id);
                    outVal.set(e.getValue());
                    context.write(outKey, outVal);
                    System.out.println("Reducer emitting INACTIVE: " + id + " -> " + e.getValue());
                    count++;
                }
            }
            System.out.println("Reducer cleanup: emitted " + count + " inactive entries");
        }
    }

    private static String[] splitCsvLine(String line) {
        if (line == null) return new String[0];
        List<String> tokens = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                tokens.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        tokens.add(cur.toString());
        return tokens.toArray(new String[0]);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TaskG <pages.csv> <access_logs.csv> <output-dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DisconnectedPeopleLast14Days");
        job.setJarByClass(TaskG.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[0]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setNumReduceTasks(1);

        boolean ok = job.waitForCompletion(true);
        System.exit(ok ? 0 : 1);
    }
}
