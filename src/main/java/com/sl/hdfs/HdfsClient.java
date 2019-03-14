package com.sl.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 上传下载
 * @author ShuLu
 */
public class HdfsClient {
    /**
     * 上传
     * @throws IOException
     */
    public  void upload() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.80.129:9000");
        configuration.set("dfs.replication","1");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("hdfs://192.168.80.129:9000/test");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        FileInputStream fileInputStream = new FileInputStream("E:\\SoftWare\\Serial IO.zip");
        IOUtils.copy(fileInputStream,fsDataOutputStream);
        //上传或者一行代码
        //fileSystem.copyFromLocalFile(new Path("E:\\SoftWare\\Serial IO.zip"), new Path("hdfs://192.168.80.129:9000/test"));
    }
    public void download() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.80.129:9000");
        configuration.set("dfs.replication","1");
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("hdfs://192.168.80.129:9000/test"));
        FileOutputStream fileOutputStream = new FileOutputStream("E:\\SoftWare\\test.download");
        IOUtils.copy(fsDataInputStream,fileOutputStream);
    }
}
