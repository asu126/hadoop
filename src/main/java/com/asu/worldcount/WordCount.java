package com.asu.worldcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private Map<String, Integer> black_list = new HashMap<String, Integer>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // Configuration conf = context.getConfiguration();

            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                try {
                    URI[] localCacheFiles = context.getCacheFiles();
                    // BufferedReader reader = new BufferedReader(new FileReader(localCacheFiles[0].getPath()));
                    FileReader reader = new FileReader(localCacheFiles[0].getPath());
                    BufferedReader breader = new BufferedReader(reader);
                    String line = null;
                    while ((line = breader.readLine()) != null) {
                        //TODO:读取每行内容进行相关的操作
                        StringTokenizer tokenizer = new StringTokenizer(line);
                        while (tokenizer.hasMoreTokens()) {
                            black_list.put(tokenizer.nextToken(), 0);
                        }
                        System.out.println(line);
                    }
                    breader.close();
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // black_list.put("weak", 0);
            // black_list.put("when", 0);
            // black_list.put("where", 0);
            // black_list.put("which", 0);
            // black_list.put("while", 0)
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                if (!black_list.containsKey(str)) {
                    word.set(str);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
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
        Configuration conf = new Configuration();
        String hdfs = "hdfs://localhost:9000";
        conf.set("fs.default.name", hdfs);
        Job job = Job.getInstance(conf, "word count");


        // String inputFileOnHDFS = "hdfs://localhost:9000/user/su/test"; // 目录或文件
        String inputFileOnHDFS = "hdfs://localhost:9000/home/su/test"; // 目录或文件
        job.addCacheFile(new Path(inputFileOnHDFS).toUri());

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("/user/su/input"));
        FileOutputFormat.setOutputPath(job, new Path("/user/su/output-008"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
