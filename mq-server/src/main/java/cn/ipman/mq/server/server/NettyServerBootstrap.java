package cn.ipman.mq.server.server;

import org.jetbrains.annotations.NotNull;
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

    @Override
    public void onApplicationEvent(@NotNull ApplicationEvent event) {
        if (event instanceof ApplicationReadyEvent) {
            Thread thread = new Thread(() -> {
                NettyMQServer server = new NettyMQServer(6666);
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
