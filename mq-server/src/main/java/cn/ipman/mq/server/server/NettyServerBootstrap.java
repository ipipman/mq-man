package cn.ipman.mq.server.server;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;


/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Component
public class NettyServerBootstrap implements ApplicationListener<ApplicationEvent> {

    @Value("${mq.server.port}")
    private int serverPort;

    @Value("${mq.server.boss.threads}")
    private int bossThreads;

    @Value("${mq.server.worker.threads}")
    private int workerThreads;

    @Override
    public void onApplicationEvent(@NotNull ApplicationEvent event) {
        if (event instanceof ApplicationReadyEvent) {
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
