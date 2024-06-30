package cn.ipman.mq.server;

import cn.ipman.mq.core.IMMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Result for MQServer.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:36
 */
@AllArgsConstructor
@Data
public class Result<T> {

    private int code;
    private T data;

    public static Result<String> ok() {
        return new Result<>(1, "OK");
    }

    public static Result<IMMessage<String>> msg(String s) {
        return new Result<>(1, IMMessage.createMessage(s, null));
    }
}
