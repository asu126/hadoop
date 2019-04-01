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
                    BufferedReader bufferReader = new BufferedReader(reader);
                    String line = null;
                    while ((line = bufferReader.readLine()) != null) {
                        //TODO:读取每行内容进行相关的操作
                        StringTokenizer tokenizer = new StringTokenizer(line);
                        while (tokenizer.hasMoreTokens()) {
                            black_list.put(tokenizer.nextToken(), 0);
                        }
                        System.out.println(line);
                    }
                    bufferReader.close();
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
        conf.addResource(new Path("file:///home/su/hadoop-2.7.6/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("file:///home/su/hadoop-2.7.6/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("file:///home/su/hadoop-2.7.6/etc/hadoop/mapred-site.xml"));
        conf.addResource(new Path("file:///home/su/hadoop-2.7.6/etc/hadoop/yarn-site.xml"));

        // Hadoop客户端提交作业时java.lang.ClassNotFoundException
        // 1.将MapReduce程序打包成一个jar文件，放到项目的根目录下。
        // 2.添加代码JobConf conf=new JobConf();conf.setJar("xxxx.jar");或者job对象的job.setJar("xxxx.jar");

        Job job = Job.getInstance(conf, "word count");
        job.setJar(System.getProperty("user.dir") + "/target/hadoop-tset-1.0-SNAPSHOT.jar");
        // String inputFileOnHDFS = "hdfs://localhost:9000/user/su/test"; // 目录或文件
        String inputFileOnHDFS = "hdfs://localhost:9000/home/su/test"; // 目录或文件
        job.addCacheFile(new Path(inputFileOnHDFS).toUri());

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        // combiner: 组合
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("/user/su/input"));
        // 输出文件不能重复，防止数据覆盖
        FileOutputFormat.setOutputPath(job, new Path("/user/su/output-008"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
