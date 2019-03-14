package com.sl.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ShuLu
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(line, " ");
        for (String w:words){
            context.write(new Text(w),new LongWritable(1));
        }
    }
}
