package com.walker.hadoop.client;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * <p>
 * HdfsPool 继承 GenericObjectPool
 * </p>
 *
 * @author mu qin
 * @date 2020/3/6
 */
public class HdfsPool extends GenericObjectPool<Hdfs> {

    public HdfsPool(PooledObjectFactory<Hdfs> factory) {
        super(factory);
    }

    public HdfsPool(PooledObjectFactory<Hdfs> factory, GenericObjectPoolConfig<Hdfs> config) {
        super(factory, config);
    }

    public HdfsPool(PooledObjectFactory<Hdfs> factory, GenericObjectPoolConfig<Hdfs> config, AbandonedConfig abandonedConfig) {
        super(factory, config, abandonedConfig);
    }
}
