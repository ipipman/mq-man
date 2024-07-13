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
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQServer {

    int port;

    public NettyMQServer(int port) {
        this.port = port;
    }

    public void run() throws Exception {

        EventLoopGroup bossGroup = new NioEventLoopGroup(2);
        EventLoopGroup workerGroup = new NioEventLoopGroup(16);

        try {
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
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            ByteBuf delimiter = Unpooled.copiedBuffer(DELIMITER.getBytes(StandardCharsets.UTF_8));
                            p.addLast(new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, delimiter));
                            p.addLast(new StringDecoder());
                            p.addLast(new StringEncoder());
                            p.addLast(new NettyMQServerHandler());
                        }
                    });

            System.out.println("netty server starting.....");
            ChannelFuture f = b.bind(port).sync();
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }


}
