package cn.ipman.mq.server.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.StandardCharsets;

import static cn.ipman.mq.metadata.model.Constants.DELIMITER;
import static cn.ipman.mq.metadata.model.Constants.MAX_FRAME_LENGTH;


/**
 * NettyMQServer是一个基于Netty框架实现的MQ服务器类。
 * 它负责初始化Netty服务器的配置，包括事件循环组、通道处理器等。
 * 以及启动和停止服务器。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQServer {

    /**
     * 服务器监听端口。
     */
    int port;
    /**
     * Boss线程组的线程数，负责接受进来的连接。
     */
    int bossThreads;
    /**
     * Worker线程组的线程数，负责处理网络IO事件。
     */
    int workerThreads;

    /**
     * 构造函数初始化服务器的端口和线程数。
     *
     * @param port       服务器监听端口
     * @param bossThreads    Boss线程组的线程数
     * @param workerThreads  Worker线程组的线程数
     */
    public NettyMQServer(int port, int bossThreads, int workerThreads) {
        this.port = port;
        this.bossThreads = bossThreads;
        this.workerThreads  = workerThreads;
    }

    /**
     * 启动服务器。
     * 这个方法会初始化Netty的事件循环组、通道处理器，并绑定端口监听。
     *
     * @throws Exception 如果启动过程中发生错误
     */
    public void run() throws Exception {

        // 创建Boss线程组和Worker线程组
        EventLoopGroup bossGroup = new NioEventLoopGroup(bossThreads);
        EventLoopGroup workerGroup = new NioEventLoopGroup(workerThreads);

        try {
            // 配置ServerBootstrap，设置各种通道选项和处理器
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 128)                     // 连接队列大小
                    .childOption(ChannelOption.TCP_NODELAY, true)       // 关闭Nagle,即时传输
                    .childOption(ChannelOption.SO_KEEPALIVE, true)      // 支持长连接
                    .childOption(ChannelOption.SO_REUSEADDR, true)      // 共享端口
                    .childOption(ChannelOption.SO_RCVBUF, 32 * 1024)    // 操作缓冲区的大小
                    .childOption(ChannelOption.SO_SNDBUF, 32 * 1024)    // 发送缓冲区的大小
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 初始化通道的处理器，包括解码器和自定义处理器
                            ChannelPipeline p = ch.pipeline();
                            ByteBuf delimiter = Unpooled.copiedBuffer(DELIMITER.getBytes(StandardCharsets.UTF_8));
                            p.addLast(new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, delimiter));
                            p.addLast(new StringDecoder());
                            p.addLast(new StringEncoder());
                            p.addLast(new NettyMQServerHandler());
                        }
                    });

            // 启动服务器并绑定端口
            System.out.println("netty server starting.....");
            ChannelFuture f = b.bind(port).sync();
            // 等待服务器关闭
            f.channel().closeFuture().sync();
        } finally {
            // 关闭线程组
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}