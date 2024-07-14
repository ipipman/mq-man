package cn.ipman.mq.metadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * 用于封装MQ服务器响应结果的模型类。
 *
 * @Author IpMan
 * @Date 2024/6/30 20:36
 */
@AllArgsConstructor
@Data
public class HttpResult<T> {

    /**
     * 响应状态码，通常用于指示请求的成功或失败。
     */
    private int code;

    /**
     * 响应数据，用于携带返回的具体数据。
     */
    private T data;

    /**
     * 创建一个成功的HTTP结果对象，默认状态码为1，消息为"OK"。
     *
     * @return 包含默认成功信息的HttpResult实例。
     */
    public static HttpResult<String> ok() {
        return new HttpResult<>(1, "OK");
    }

    /**
     * 创建一个带有自定义消息的HTTP结果对象，状态码为1。
     *
     * @param msg 自定义的消息字符串。
     * @return 包含自定义消息的HttpResult实例。
     */
    public static HttpResult<String> ok(String msg) {
        return new HttpResult<>(1, msg);
    }

    /**
     * 创建一个带有Message类型数据的HTTP结果对象，状态码为1。
     *
     * @param message Message类型的数据。
     * @return 包含Message数据的HttpResult实例。
     */
    public static HttpResult<Message<?>> msg(Message<?> message) {
        return new HttpResult<>(1, message);
    }

    /**
     * 创建一个带有Message列表类型数据的HTTP结果对象，状态码为1。
     *
     * @param messages Message列表类型的数据。
     * @return 包含Message列表数据的HttpResult实例。
     */
    public static HttpResult<List<Message<?>>> msg(List<Message<?>> messages) {
        return new HttpResult<>(1, messages);
    }

    /**
     * 创建一个带有统计信息类型的HTTP结果对象，状态码为1。
     *
     * @param statistical 统计信息类型的数据。
     * @return 包含统计信息数据的HttpResult实例。
     */
    public static HttpResult<Statistical> stat(Statistical statistical) {
        return new HttpResult<>(1, statistical);
    }
}
