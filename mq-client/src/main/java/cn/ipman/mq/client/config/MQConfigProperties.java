package cn.ipman.mq.client.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 客户端配置
 *
 * @Author IpMan
 * @Date 2024/7/14 10:21
 */

@Data
@Configuration
@ConfigurationProperties(prefix = "mq.client")
public class MQConfigProperties {

    private String host = "127.0.0.1";
    private int port = 8765;
    private int poolMaxTotal = 10;
    private int poolMaxIdle = 5;
    private int poolMinIdle = 2;

}
