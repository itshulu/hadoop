package com.sl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @author ShuLu
 */
public class HdfsClientApi {
    private FileSystem fs=null;
    @Before
    public void getFS() throws Exception {
        // Sets the system property indicated by the specified key.
        // 设置指定键指示的系统属性。
        System.setProperty("hadoop.home.dir", "D:\\winutils\\hadoop-2.8.3");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.80.129:9000");
        configuration.set("dfs.replication","1");
        //获取一个具体的 文件系统客户端实例
        //fs = FileSystem.get(configuration);
        //可以指定访问hdfs的客户端身份
        fs = FileSystem.get(new URI("hdfs://192.168.80.129:9000/"),configuration,"shulu");
    }
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(new Path("E:\\SoftWare\\Serial IO.zip"), new Path("hdfs://192.168.80.129:9000/test"));
    }
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(new Path("hdfs://192.168.80.129:9000/test"),new Path("E:\\SoftWare\\b"));
    }
    @Test
    public void testRemoveFile() throws IOException {
        boolean b = fs.delete(new Path("/test"), true);
        System.out.println(b?"success":"failure");
    }
    @Test
    public void testMkdir() throws IOException {
        fs.mkdirs(new Path("/aa"));
    }
    @Test
    public void testRename() throws IOException {
        fs.rename(new Path("/aa"),new Path("/bb"));
    }
    @Test
    public void testSelectFile() throws IOException {
        //列出文件夹下的文件
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            System.out.println(locatedFileStatusRemoteIterator.next().getPath().getName());
        }
        //可以列出文件夹并且可以判断列出的是文件夹还是文件
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses ){
            System.out.println(fileStatus.getPath().getName()+(fileStatus.isDirectory()?"是文件夹":"是文件"));
        }
    }
}
