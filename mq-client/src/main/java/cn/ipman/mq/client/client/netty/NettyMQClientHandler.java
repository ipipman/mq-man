package cn.ipman.mq.client.client.netty;

import cn.ipman.mq.metadata.model.NettyRequest;
import cn.ipman.mq.metadata.model.NettyResponse;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static cn.ipman.mq.metadata.model.Constants.DELIMITER;


/**
 * NettyMQClientHandler 是 Netty 客户端的处理器，用于处理消息的发送和接收。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQClientHandler extends SimpleChannelInboundHandler<String> {

    /**
     * 请求ID生成器，用于唯一标识每个请求。
     */
    private static final AtomicLong requestIdGenerator = new AtomicLong(0);
    /**
     * 存储待处理响应的映射，键为请求ID，值为CompletableFuture，用于异步处理响应。
     */
    private final ConcurrentHashMap<Long, CompletableFuture<String>> paddingRequests = new ConcurrentHashMap<>();

    /**
     * 处理接收到的通道消息。
     *
     * @param ctx 通道上下文，用于通道操作。
     * @param msg 接收到的消息字符串。
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        // 解析接收到的消息为NettyResponse对象。
        NettyResponse<?> response = JSON.parseObject(msg, new TypeReference<NettyResponse<?>>(){});
        // 获取响应的请求ID。
        long requestId = response.getTraceId();
        // 根据请求ID从映射中移除并返回对应的CompletableFuture。
        CompletableFuture<String> future = paddingRequests.remove(requestId);
        if (future != null) {
            // 如果CompletableFuture存在，则完成它，即将消息字符串作为结果完成。
            future.complete(msg);
        }
    }

    /**
     * 发送消息到服务端并返回一个CompletableFuture用于异步接收响应。
     *
     * @param channel        用于发送消息的Netty通道。
     * @param action         操作动作。
     * @param params         操作参数。
     * @param message        要发送的消息内容。
     * @return 一个CompletableFuture，用于异步接收服务端的响应消息。
     */
    public CompletableFuture<String> sendMessage(Channel channel, String action,
                                                 Map<String, String> params, String message) {
        // 生成唯一的请求ID。
        long requestId = requestIdGenerator.getAndIncrement();
        // 创建NettyRequest对象，设置请求ID、动作、参数和消息体。
        NettyRequest<String> request = new NettyRequest<>();
        request.setTraceId(requestId);
        request.setAction(action);
        request.setParams(params);
        request.setBody(message);
        // 将NettyRequest对象序列化为JSON字符串。
        String jsonRequest = JSON.toJSONString(request);

        // 创建一个新的CompletableFuture用于接收响应。
        CompletableFuture<String> future = new CompletableFuture<>();
        // 将请求ID和CompletableFuture存入映射中，用于后续接收响应。
        paddingRequests.put(requestId, future);
        // 将序列化后的JSON字符串加上分隔符写入通道并刷新。
        channel.writeAndFlush(jsonRequest + DELIMITER);
        // 返回CompletableFuture。
        return future;
    }
}