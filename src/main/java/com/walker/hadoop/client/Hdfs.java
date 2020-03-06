package com.walker.hadoop.client;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * <p>
 * hdfs 连接类，最底层对 hdfs 的操作
 * </p>
 *
 * @author mu qin
 * @date 2020/3/6
 */
@Slf4j
public class Hdfs {

    private FileSystem fs;
    private String coreResource;
    private String hdfsResource;
    private final String url;
    private static final String NAME = "fs.hdfs.impl";

    public Hdfs(String url) {
        this.url = url;
    }

    public void open() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", url);
            fs = FileSystem.get(conf);
            log.info("[Hadoop]创建实例成功");
        } catch (Exception e) {
            log.error("[Hadoop]创建实例失败", e);
        }
    }

    public void close() {
        try {
            if (null != fs) {
                fs.close();
                log.info("[Hadoop]关闭实例成功");
            }
        } catch (IOException e) {
            log.error("[Hadoop]关闭实例失败", e);
        }
    }

    public boolean isConnected() throws IOException {
        return fs.exists(new Path("/"));
    }

    public boolean exists(String path) throws IOException {
        Path hdfsPath = new Path(path);
        return fs.exists(hdfsPath);
    }

    public FileStatus getFileStatus(String path) throws IOException {
        Path hdfsPath = new Path(path);
        return fs.getFileStatus(hdfsPath);
    }

    public ContentSummary getContentSummary(String path) throws IOException {
        ContentSummary contentSummary = null;
        Path hdfsPath = new Path(path);
        if (fs.exists(hdfsPath)) {
            contentSummary = fs.getContentSummary(hdfsPath);
        }
        return contentSummary;
    }

    public List<String> listFileName() throws IOException {
        List<String> res = Lists.newArrayList();
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            res.add(fileStatus.getPath() + "：类型--" + (fileStatus.isDirectory() ? "文件夹" : "文件"));
        }
        return res;
    }
}
