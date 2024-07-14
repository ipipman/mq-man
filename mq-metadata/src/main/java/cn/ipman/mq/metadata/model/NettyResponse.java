package cn.ipman.mq.metadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class NettyResponse<T> {

    /**
     *  跟踪ID，用于问题追踪和日志记录，确保请求和响应的一致性。
     */
    private long traceId;

    /**
     * 响应代码，用于表示响应的状态或结果，例如成功、错误等。
     */
    private int code;

    /**
     * 响应数据，承载具体的业务数据或信息。
     */
    private T data;
}
