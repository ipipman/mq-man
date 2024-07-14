package cn.ipman.mq.server.server;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;


/**
 * MQ服务器启动器，负责在Spring应用程序准备就绪时启动Netty服务器。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Component
public class NettyServerBootstrap implements ApplicationListener<ApplicationEvent> {

    /**
     * 服务器端口，由Spring Boot的属性配置提供。
     */
    @Value("${mq.server.port}")
    private int serverPort;

    /**
     * Netty的Boss线程池大小，由Spring Boot的属性配置提供。
     */
    @Value("${mq.server.boss.threads}")
    private int bossThreads;

    /**
     * Netty的Worker线程池大小，由Spring Boot的属性配置提供。
     */
    @Value("${mq.server.worker.threads}")
    private int workerThreads;

    /**
     * 处理应用事件。当应用准备就绪时，启动Netty服务器。
     *
     * @param event 应用事件。如果事件为ApplicationReadyEvent类型，则启动Netty服务器。
     */
    @Override
    public void onApplicationEvent(@NotNull ApplicationEvent event) {
        if (event instanceof ApplicationReadyEvent) {
            // 在新线程中启动Netty服务器，以避免阻塞应用启动过程
            Thread thread = new Thread(() -> {
                NettyMQServer server = new NettyMQServer(serverPort, bossThreads, workerThreads);
                try {
                    server.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            thread.start();
        }
    }
}
