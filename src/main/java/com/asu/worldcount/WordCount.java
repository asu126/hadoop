package com.asu.worldcount;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
}

class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

public class WordCount {

    public static void main(String[] args) throws Exception {
        String hdfs = "hdfs://localhost:9000";
        Configuration conf = new Configuration();


        conf.addResource(new Path("file:///home/su/hadoop-2.7.6/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("file:///home/su/hadoop-2.7.6/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("file:///home/su/hadoop-2.7.6/etc/hadoop/mapred-site.xml"));
        conf.addResource(new Path("file:///home/su/hadoop-2.7.6/etc/hadoop/yarn-site.xml"));
        // conf.set("fs.default.name", hdfs);

        Job job = new Job(conf, "wordcount");

        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // FileInputFormat.addInputPath(job, new Path(args[0]))
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job,  new Path("/user/su/input"));
        FileOutputFormat.setOutputPath(job,  new Path("/user/su/output-006"));

        job.waitForCompletion(true);
    }

}
