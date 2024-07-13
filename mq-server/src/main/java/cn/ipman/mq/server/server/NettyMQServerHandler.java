package cn.ipman.mq.server.server;

import cn.ipman.mq.metadata.model.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


import java.util.List;

import static cn.ipman.mq.metadata.model.Constants.DELIMITER;


/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 解析请求
        String message = (String) msg;
        System.out.println("Received message: " + message);
        NettyRequest<?> request = JSON.parseObject(message, new TypeReference<NettyRequest<?>>() {
        });
        // 处理请求并返回
        NettyResponse<?> response = handlerRequest(request);
        String jsonResponse = JSON.toJSONString(response) + DELIMITER;
        ctx.writeAndFlush(jsonResponse);
    }

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

    private NettyResponse<?> handleSend(NettyRequest<?> request) {
        // 调用 MessageQueue.send 方法
        String topic = request.getParams().get("t");
        String result = String.valueOf(MessageQueue.send(topic,
                JSON.parseObject((String) request.getBody(), new TypeReference<Message<String>>(){})));
        return new NettyResponse<>(request.getTraceId(), 1, "msg" + result);
    }

    private NettyResponse<?> handleReceive(NettyRequest<?> request) {
        // 调用 MessageQueue.receive 方法
        System.out.println("===>>===>>===>>===>>===>> " + request);
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        Message<?> message = MessageQueue.receive(topic, consumerId);
        return new NettyResponse<>(request.getTraceId(), 1, message);
    }

    private NettyResponse<?> handleBatchReceive(NettyRequest<?> request) {
        // 调用 MessageQueue.batchReceive 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        int size = Integer.parseInt(request.getParams().getOrDefault("size", "1000"));
        List<Message<?>> messages = MessageQueue.batchReceive(topic, consumerId, size);
        return new NettyResponse<>(request.getTraceId(), 1, messages);
    }

    private NettyResponse<?> handleAck(NettyRequest<?> request) {
        // 调用 MessageQueue.ack 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        int offset = Integer.parseInt(request.getParams().get("offset"));
        String result = String.valueOf(MessageQueue.ack(topic, consumerId, offset));
        return new NettyResponse<>(request.getTraceId(), 1, result);
    }

    private NettyResponse<?> handleSubscribe(NettyRequest<?> request) {
        // 调用 MessageQueue.sub 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        MessageQueue.sub(new Subscription(topic, consumerId, -1));
        return new NettyResponse<>(request.getTraceId(), 1, null);
    }

    private NettyResponse<?> handleUnSubscribe(NettyRequest<?> request) {
        // 调用 MessageQueue.unsub 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        MessageQueue.unsub(new Subscription(topic, consumerId, -1));
        return new NettyResponse<>(request.getTraceId(), 1, null);
    }

    private NettyResponse<?> handleStat(NettyRequest<?> request) {
        // 调用 MessageQueue.stat 方法
        String topic = request.getParams().get("t");
        String consumerId = request.getParams().get("cid");
        Statistical stat = MessageQueue.stat(topic, consumerId);
        return new NettyResponse<>(request.getTraceId(), 1, stat);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

}
