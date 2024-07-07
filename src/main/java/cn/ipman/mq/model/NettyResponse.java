package cn.ipman.mq.model;

import lombok.*;



@Data
@AllArgsConstructor
@NoArgsConstructor
public class NettyResponse<T>  {

    private long traceId;
    private int code;
    private T data;
}
