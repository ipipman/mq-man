package cn.ipman.mq.metadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;



@Data
@AllArgsConstructor
@NoArgsConstructor
public class NettyResponse<T>  {

    private long traceId;
    private int code;
    private T data;
}
