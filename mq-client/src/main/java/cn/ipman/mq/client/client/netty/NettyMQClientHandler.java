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
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQClientHandler extends SimpleChannelInboundHandler<String> {

    private static final AtomicLong requestIdGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<Long, CompletableFuture<String>> paddingRequests = new ConcurrentHashMap<>();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        NettyResponse<?> response = JSON.parseObject(msg, new TypeReference<NettyResponse<?>>(){});
        long requestId = response.getTraceId();
        CompletableFuture<String> future = paddingRequests.remove(requestId);
        if (future != null) {
            future.complete(msg);
        }
    }

    public CompletableFuture<String> sendMessage(Channel channel, String action,
                                                           Map<String, String> params, String message) {
        long requestId = requestIdGenerator.getAndIncrement();
        NettyRequest<String> request = new NettyRequest<>();
        request.setTraceId(requestId);
        request.setAction(action);
        request.setParams(params);
        request.setBody(message);
        String jsonRequest = JSON.toJSONString(request);

        CompletableFuture<String> future = new CompletableFuture<>();
        paddingRequests.put(requestId, future);
        channel.writeAndFlush( jsonRequest + DELIMITER);
        return future;
    }
}
