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
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static cn.ipman.mq.metadata.model.Constants.DELIMITER;
import static cn.ipman.mq.metadata.model.Constants.MAX_FRAME_LENGTH;


/**
 * NettyMQClient 使用Netty框架实现的MQ客户端。
 * 用于建立与服务端的连接，并发送与接收消息。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQClient {

    String host;
    int port;
    NettyMQClientHandler clientHandler;
    int timeout = 5_000; // 超时时间5s
    private Channel channel;
    private EventLoopGroup group;

    /**
     * 构造函数初始化客户端。
     * @param host 服务端主机地址
     * @param port 服务端端口号
     */
    public NettyMQClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.clientHandler = new NettyMQClientHandler();
    }

    /**
     * 启动客户端，建立与服务端的连接。
     */
    public void start() {
        group = new NioEventLoopGroup(10);
        try {
            Bootstrap b = new Bootstrap();
            b.group(group);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeout); // 连接超时设置
            b.option(ChannelOption.SO_KEEPALIVE, true);     // 开启TCP KeepAlive
            b.handler(new LoggingHandler(LogLevel.INFO));            // 日志处理器
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline p = ch.pipeline();
                    ByteBuf delimiter = Unpooled.copiedBuffer(DELIMITER.getBytes(StandardCharsets.UTF_8));
                    p.addLast(new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, delimiter)); // 消息分帧解码器
                    p.addLast(new ReadTimeoutHandler(timeout, TimeUnit.MILLISECONDS)); // 读超时处理器
                    p.addLast(new StringDecoder());      // 字符串解码器
                    p.addLast(new StringEncoder());      // 字符串编码器
                    p.addLast(clientHandler);            // 自定义消息处理处理器
                }
            });

            ChannelFuture f = b.connect(host, port).sync();
            channel = f.channel();
        } catch (Exception e) {
            group.shutdownGracefully();
            throw new RuntimeException(e);
        }
    }

    /**
     * 检查客户端连接是否活跃。
     * @return 连接活跃返回true，否则返回false。
     */
    public boolean isActive() {
        return channel != null && channel.isActive();
    }


    /**
     * 关闭客户端连接及线程组。
     */
    public void close() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    /**
     * 发送消息到服务端。
     * @param action 消息操作类型。
     * @param params 消息额外参数。
     * @param message 待发送的消息对象。
     * @return 完成发送的消息ID的CompletableFuture。
     */
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
