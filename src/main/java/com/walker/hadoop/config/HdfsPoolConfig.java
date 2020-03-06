package com.walker.hadoop.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * <p>
 * 继承 GenericObjectPoolConfig, 对连接池常规属性的配置
 * GenericObjectPoolConfig - 参数配置类
 * </p>
 *
 * @author mu qin
 * @date 2020/3/6
 */
public class HdfsPoolConfig extends GenericObjectPoolConfig {

    public HdfsPoolConfig() {
    }

    /**
     * @param testWhileIdle                在空闲时检查有效性，默认false
     * @param minEvictableIdleTimeMillis   逐出连接的最小空闲时间
     * @param timeBetweenEvictionRunMillis 逐出扫描的时间间隔（毫秒），如果为负数则不运行逐出线程，默认为-1
     * @param numTestsPerEvictionRun       每次逐出检查时，逐出的最大数目
     */
    public HdfsPoolConfig(boolean testWhileIdle, long minEvictableIdleTimeMillis, long timeBetweenEvictionRunMillis,
                          int numTestsPerEvictionRun) {
        this.setTestWhileIdle(testWhileIdle);
        this.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        this.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunMillis);
        this.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
    }
}
