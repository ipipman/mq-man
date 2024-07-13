package cn.ipman.mq.client.client.netty;

import cn.ipman.mq.metadata.model.Message;
import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static cn.ipman.mq.metadata.model.Constants.DELIMITER;
import static cn.ipman.mq.metadata.model.Constants.MAX_FRAME_LENGTH;


/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQClient {

    String host;
    int port;
    NettyMQClientHandler clientHandler;
    int timeout = 2_000;

    private Channel channel;
    private EventLoopGroup group;

    public NettyMQClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.clientHandler = new NettyMQClientHandler();
    }

    public void start() {
        group = new NioEventLoopGroup(10);
        try {
            Bootstrap b = new Bootstrap();
            b.group(group);
            b.channel(NioSocketChannel.class);
            //b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeout); // 连接超时
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new LoggingHandler(LogLevel.INFO));
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    ByteBuf delimiter = Unpooled.copiedBuffer(DELIMITER.getBytes(StandardCharsets.UTF_8));
                    p.addLast(new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, delimiter));
                    //p.addLast(new ReadTimeoutHandler(timeout, TimeUnit.MILLISECONDS)); // 读超时
                    p.addLast(new StringDecoder());
                    p.addLast(new StringEncoder());
                    p.addLast(clientHandler);
                }
            });

            ChannelFuture f = b.connect(host, port).sync();
            channel = f.channel();
        } catch (Exception e) {
            group.shutdownGracefully();
            throw new RuntimeException(e);
        }
    }

    public boolean isActive() {
        return channel != null && channel.isActive();
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    public CompletableFuture<String> sendMessage(String action, Map<String, String> params, Message<?> message) {
        if (channel != null && channel.isActive()) {
            String jsonMessage = JSON.toJSONString(message);
            return clientHandler.sendMessage(channel, action, params, jsonMessage);
        } else {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Channel is not active"));
            return future;
        }
    }
}
