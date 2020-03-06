package com.walker.hadoop.config;

import com.walker.hadoop.client.HdfsClient;
import com.walker.hadoop.client.HdfsFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <p>
 * 配置类，管理属性值注入和声明 Bean 的作用
 * </p>
 *
 * @author mu qin
 * @date 2020/3/6
 */
@Configuration
public class HdfsConfig {

    @Value("${hadoop.hdfs.ip}")
    private String hdfsIp;
    @Value("${hadoop.hdfs.port}")
    private String hdfsPort;
    @Value("${hadoop.hdfs.maxTotal}")
    private int maxTotal;
    @Value("${hadoop.hdfs.maxIdle}")
    private int maxIdle;
    @Value("${hadoop.hdfs.minIdle}")
    private int minIdle;
    @Value("${hadoop.hdfs.maxWaitMillis}")
    private int maxWaitMillis;
    @Value("${hadoop.hdfs.testWhileIdle}")
    private boolean testWhileIdle;
    @Value("${hadoop.hdfs.minEvictableIdleTimeMillis}")
    private long minEvictableIdleTimeMillis;
    @Value("${hadoop.hdfs.timeBetweenEvictionRunsMillis}")
    private long timeBetweenEvictionRunsMillis;
    @Value("${hadoop.hdfs.numTestsPerEvictionRun}")
    private int numTestsPerEvictionRun;

    @Bean(initMethod = "init", destroyMethod = "stop")
    public HdfsClient hdfsClient() {
        return new HdfsClient();
    }

    @Bean
    public HdfsPoolConfig hdfsPoolConfig() {
        HdfsPoolConfig hdfsPoolConfig = new HdfsPoolConfig();
        hdfsPoolConfig.setTestWhileIdle(testWhileIdle);
        hdfsPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        hdfsPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        hdfsPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        hdfsPoolConfig.setMaxTotal(maxTotal);
        hdfsPoolConfig.setMaxIdle(maxIdle);
        hdfsPoolConfig.setMinIdle(minIdle);
        hdfsPoolConfig.setMaxWaitMillis(maxWaitMillis);

        return hdfsPoolConfig;
    }

    @Bean
    public HdfsFactory hdfsFactory() {
        return new HdfsFactory("hdfs://" + hdfsIp + ":" + hdfsPort);
    }
}
