package cn.ipman.mq.metadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class NettyRequest<T> {
    /**
     * 请求跟踪ID，用于唯一标识一个请求，便于问题追踪和日志记录。
     */
    private long traceId;

    /**
     * 请求的操作动作，指明此次请求的具体行为，例如发送消息、订阅主题等。
     */
    private String action;

    /**
     * 请求参数映射，用于存放请求所需的额外参数，以键值对形式提供。
     */
    private Map<String, String> params;

    /**
     * 请求的主体内容，具体的请求数据，其类型由泛型T指定。
     */
    private T body;



}
