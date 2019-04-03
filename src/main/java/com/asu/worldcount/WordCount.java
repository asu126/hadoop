package com.asu.worldcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
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
        // 与-libjars xx1.jar,xx2.jar 的区别???
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
