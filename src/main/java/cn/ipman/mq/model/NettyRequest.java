package cn.ipman.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class NettyRequest<T> {

    private long traceId;
    private String action;
    private Map<String, String> params;
    private T body;


}
