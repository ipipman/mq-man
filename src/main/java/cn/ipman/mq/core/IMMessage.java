package cn.ipman.mq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * IM Message model.
 *
 * @Author IpMan
 * @Date 2024/6/29 19:12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IMMessage<T> {

    // private String topic;
    private long id;
    private T body;
    private Map<String, String> headers; // 系统属性, X-version = 1.0
    // private Map<String, String> properties; // 业务属性

}
