package com.walker.hadoop.client;

import com.walker.hadoop.config.HdfsPoolConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * <p>
 * 业务代码使用这个类中封装的方法
 * </p>
 *
 * @author mu qin
 * @date 2020/3/6
 */
@Slf4j
public class HdfsClient {

    private HdfsPool hdfsPool;
    @Autowired
    private HdfsPoolConfig hdfsPoolConfig;
    @Autowired
    private HdfsFactory hdfsFactory;

    public void init() {
        hdfsPool = new HdfsPool(hdfsFactory, hdfsPoolConfig);
    }

    public void stop() {
        hdfsPool.close();
    }

    public long getPathSize(String path) throws Exception {
        Hdfs hdfs = null;
        try {
            hdfs = hdfsPool.borrowObject();
            return hdfs.getContentSummary(path).getLength();
        } catch (Exception e) {
            log.error("[HDFS]获取路径大小失败", e);
            throw e;
        } finally {
            if (null != hdfs) {
                hdfsPool.returnObject(hdfs);
            }
        }
    }

    public List<String> getBasePath() {
        Hdfs hdfs = null;
        try {
            hdfs = hdfsPool.borrowObject();
            return hdfs.listFileName();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (null != hdfs) {
                hdfsPool.returnObject(hdfs);
            }
        }
    }

}
