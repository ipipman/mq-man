package cn.ipman.mq.core;

import lombok.Data;

import java.util.Map;

/**
 * IM Message model.
 *
 * @Author IpMan
 * @Date 2024/6/29 19:12
 */
@Data
public class IMMessage<T> {

    // private String topic;
    private T body;
    private Long id;
    private Map<String, String> headers; // 系统属性, X-version = 1.0
    // private Map<String, String> properties; // 业务属性

}
