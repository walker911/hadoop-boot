package com.walker.hadoop.client;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;

/**
 * <p>
 * 实现 PooledObjectFactory 接口，创建连接 hdfs 的实例
 * PooledObjectFactory 池对象工厂
 * </p>
 *
 * @author mu qin
 * @date 2020/3/6
 */
public class HdfsFactory implements PooledObjectFactory<Hdfs> {

    private final String url;

    public HdfsFactory(String url) {
        this.url = url;
    }

    /**
     * 创建对象
     *
     * @return
     * @throws Exception
     */
    @Override
    public PooledObject<Hdfs> makeObject() throws Exception {
        Hdfs hdfs = new Hdfs(url);
        hdfs.open();
        return new DefaultPooledObject<>(hdfs);
    }

    /**
     * 销毁对象
     *
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void destroyObject(PooledObject<Hdfs> pooledObject) throws Exception {
        Hdfs hdfs = pooledObject.getObject();
        hdfs.close();
    }

    /**
     * 检查一个对象是否有效，无效会被销毁
     *
     * @param pooledObject
     * @return
     */
    @Override
    public boolean validateObject(PooledObject<Hdfs> pooledObject) {
        Hdfs hdfs = pooledObject.getObject();
        try {
            return hdfs.isConnected();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void activateObject(PooledObject<Hdfs> pooledObject) throws Exception {

    }

    /**
     * 钝化对象
     *
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void passivateObject(PooledObject<Hdfs> pooledObject) throws Exception {

    }
}
