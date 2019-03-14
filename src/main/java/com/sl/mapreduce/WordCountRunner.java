package com.sl.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


/**
 * job 用来描述使用那个mapreduce （指定输入文件以及目标文件地址）最后提交job
 *
 * @author ShuLu
 */
public class WordCountRunner {
    private static final String DEFAULT_HDFS_PATH = "hdfs://192.168.80.129:9000/";
    private static Logger logger = Logger.getLogger(WordCountRunner.class);
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\winutils\\hadoop-2.8.3");
        System.setProperty("HADOOP_USER_NAME", "shulu");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //指定此类中的资源所在jar包
        job.setJarByClass(WordCountRunner.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //Mapper类输出的k，v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //Reducer类输出的k，v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定要处理的文件路径
        FileInputFormat.setInputPaths(job,new Path(DEFAULT_HDFS_PATH+"wc/srcdata"));
        // 指定输出结果
        FileOutputFormat.setOutputPath(job,new Path(DEFAULT_HDFS_PATH+"/wc/output"));
        //本地处理
        //FileInputFormat.setInputPaths(job,  new Path("E:/wc/srcdata"));
        //FileOutputFormat.setOutputPath(job, new Path("E:/wc/output"));
        job.waitForCompletion(true);
    }
}
