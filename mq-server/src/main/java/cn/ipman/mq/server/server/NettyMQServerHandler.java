package cn.ipman.mq.server.server;

import cn.ipman.mq.metadata.model.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


import java.util.List;

import static cn.ipman.mq.metadata.model.Constants.DELIMITER;

/**
 * MQ服务器的Netty处理程序适配器。
 * 该类负责处理Netty通道中的入站事件，主要是解析客户端发送的消息并进行相应的业务处理。
 */
public class NettyMQServerHandler extends ChannelInboundHandlerAdapter {

    /**
     * 当通道可读时调用，用于处理接收到的消息。
     *
     * @param ctx 通道上下文，用于发送响应。
     * @param msg 接收到的消息对象。
     * @throws Exception 如果处理过程中发生异常。
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 解析请求消息
        // 解析请求
        String message = (String) msg;
        System.out.println("Received message: " + message);
        NettyRequest<?> request = JSON.parseObject(message, new TypeReference<NettyRequest<?>>() {
        });
        // 处理请求并构造响应
        // 处理请求并返回
        NettyResponse<?> response = handlerRequest(request);
        // 将响应转换为JSON字符串并发送给客户端
        String jsonResponse = JSON.toJSONString(response) + DELIMITER;
        ctx.writeAndFlush(jsonResponse);
    }

    /**
     * 根据请求的动作处理请求。
     *
     * @param request 客户端的请求。
     * @return 构造的响应对象。
     */
    private NettyResponse<?> handlerRequest(NettyRequest<?> request) {
        return switch (request.getAction()) {
            case "send" -> handleSend(request);
            case "receive" -> handleReceive(request);
            case "batch-receive" -> handleBatchReceive(request);
            case "ack" -> handleAck(request);
            case "sub" -> handleSubscribe(request);
            case "unsub" -> handleUnSubscribe(request);
            case "stat" -> handleStat(request);
            default -> new NettyResponse<>(request.getTraceId(), 0, "Unknown action");
        };
    }

    /**
     * 处理发送消息的请求。
     *
     * @param request 包含发送消息请求信息的对象。
     * @return 构造的响应对象。
     */
    private NettyResponse<?> handleSend(NettyRequest<?> request) {
        // 调用MessageQueue发送消息
        // 调用 MessageQueue.send 方法
        String topic = request.getParams().get("t");
        String result = String.valueOf(MessageQueue.send(topic,
                JSON.parseObject((String) request.getBody(), new TypeReference<Message<String>>(){})));
        return new NettyResponse<>(request.getTraceId(), 1, "msg" + result);
    }

    /**
     * 处理接收消息的请求。
     *
     * @param request 包含接收消息请求信息的对象。
     * @return 构造的响应对象。
     */
    private NettyResponse<?> handleReceive(NettyRequest<?> request) {
        // 调用MessageQueue接收消息
        // 调用 MessageQueue.receive 方法
        System.out.println("===>>===>>===>>===>>===>> " + request);
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        Message<?> message = MessageQueue.receive(topic, consumerId);
        return new NettyResponse<>(request.getTraceId(), 1, message);
    }

    /**
     * 处理批量接收消息的请求。
     *
     * @param request 包含批量接收消息请求信息的对象。
     * @return 构造的响应对象。
     */
    private NettyResponse<?> handleBatchReceive(NettyRequest<?> request) {
        // 调用MessageQueue批量接收消息
        // 调用 MessageQueue.batchReceive 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        int size = Integer.parseInt(request.getParams().getOrDefault("size", "1000"));
        List<Message<?>> messages = MessageQueue.batchReceive(topic, consumerId, size);
        return new NettyResponse<>(request.getTraceId(), 1, messages);
    }

    /**
     * 处理消息确认（ACK）的请求。
     *
     * @param request 包含消息确认请求信息的对象。
     * @return 构造的响应对象。
     */
    private NettyResponse<?> handleAck(NettyRequest<?> request) {
        // 调用MessageQueue确认消息消费
        // 调用 MessageQueue.ack 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        int offset = Integer.parseInt(request.getParams().get("offset"));
        String result = String.valueOf(MessageQueue.ack(topic, consumerId, offset));
        return new NettyResponse<>(request.getTraceId(), 1, result);
    }

    /**
     * 处理订阅消息的请求。
     *
     * @param request 包含订阅消息请求信息的对象。
     * @return 构造的响应对象。
     */
    private NettyResponse<?> handleSubscribe(NettyRequest<?> request) {
        // 调用MessageQueue订阅消息
        // 调用 MessageQueue.sub 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        MessageQueue.sub(new Subscription(topic, consumerId, -1));
        return new NettyResponse<>(request.getTraceId(), 1, null);
    }

    /**
     * 处理取消订阅消息的请求。
     *
     * @param request 包含取消订阅消息请求信息的对象。
     * @return 构造的响应对象。
     */
    private NettyResponse<?> handleUnSubscribe(NettyRequest<?> request) {
        // 调用MessageQueue取消订阅消息
        // 调用 MessageQueue.unsub 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        MessageQueue.unsub(new Subscription(topic, consumerId, -1));
        return new NettyResponse<>(request.getTraceId(), 1, null);
    }

    /**
     * 处理查询消息统计信息的请求。
     *
     * @param request 包含查询统计信息请求信息的对象。
     * @return 构造的响应对象。
     */
    private NettyResponse<?> handleStat(NettyRequest<?> request) {
        // 调用MessageQueue查询消息统计信息
        // 调用 MessageQueue.stat 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        Statistical stat = MessageQueue.stat(topic, consumerId);
        return new NettyResponse<>(request.getTraceId(), 1, stat);
    }

    /**
     * 当通道发生异常时调用。
     *
     * @param ctx 通道上下文。
     * @param cause 异常原因。
     * @throws Exception 如果处理异常过程中发生新的异常。
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
