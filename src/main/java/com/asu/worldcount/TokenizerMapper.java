package com.asu.worldcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

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

    public void map(LongWritable key, Text value, Context context
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

