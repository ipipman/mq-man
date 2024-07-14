package cn.ipman.mq.client.client.netty;


import cn.ipman.mq.client.client.ClientService;
import cn.ipman.mq.metadata.model.Message;
import cn.ipman.mq.metadata.model.NettyResponse;
import cn.ipman.mq.metadata.model.Statistical;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Netty客户端实现类，作为MQ客户端的具体实现，负责与Netty服务端进行通信。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyClientImpl implements ClientService {

    /**
     * 服务端主机地址。
     */
    String host;

    /**
     * 服务端端口号。
     */
    int port;

    /**
     * 客户端连接池，管理NettyMQClient的连接。
     */
    NettyMQClientPool clientPool;

    /**
     * 构造函数，初始化Netty客户端实例。
     *
     * @param host     服务端主机地址。
     * @param port     服务端端口号。
     * @param maxTotal 连接池最大总数。
     * @param maxIdle  连接池最大空闲数。
     * @param minIdle  连接池最小空闲数。
     */
    public NettyClientImpl(String host, int port, int maxTotal, int maxIdle, int minIdle) {
        this.host = host;
        this.port = port;
        this.clientPool = new NettyMQClientPool(host, port, maxTotal, maxIdle, minIdle);
    }

    /**
     * 使用客户端连接池中的客户端执行给定的操作。
     *
     * @param function 定义如何使用客户端执行特定操作的函数式接口。
     * @param <T>      操作的返回类型。
     * @return 操作的执行结果。
     */
    private <T> T executeWithClient(Function<NettyMQClient, T> function) {
        NettyMQClient client = null;
        try {
            client = clientPool.borrowClient();
            return function.apply(client);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (client != null) {
                clientPool.returnClient(client);
            }
        }
    }


    /**
     * 发送消息到指定主题。
     *
     * @param topic    消息主题。
     * @param message  待发送的消息。
     * @return 发送是否成功的布尔值。
     */
    @Override
    public Boolean send(String topic, Message<?> message) {
        return executeWithClient(client -> {
            try {
                Map<String, String> params = Map.of("t", topic);
                CompletableFuture<String> future = client.sendMessage("send", params, message);
                NettyResponse<String> response = JSON.parseObject(future.get(),
                        new TypeReference<NettyResponse<String>>() {
                        });
                System.out.println("【send】 Received response: " + response);
                return response.getCode() == 1;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        });
    }

    /**
     * 订阅指定主题。
     *
     * @param topic    消息主题。
     * @param consumerId 消费者ID。
     */
    @Override
    public void subscribe(String topic, String consumerId) {
        executeWithClient(client -> {
            try {
                Map<String, String> params = Map.of("t", topic, "cid", consumerId);
                CompletableFuture<String> future = client.sendMessage("sub", params, null);
                NettyResponse<String> response = JSON.parseObject(future.get(),
                        new TypeReference<NettyResponse<String>>() {
                        });
                System.out.println("【sub】 Received response: " + response);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    /**
     * 从指定主题接收消息。
     *
     * @param topic    消息主题。
     * @param consumerId 消费者ID。
     * @param <T>      消息体的类型。
     * @return 接收到的消息。
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Message<T> receive(String topic, String consumerId) {
        return executeWithClient(client -> {
            try {
                Map<String, String> params = Map.of("t", topic, "cid", consumerId);
                CompletableFuture<String> future = client.sendMessage("receive", params, null);
                NettyResponse<Message<String>> response = JSON.parseObject(future.get(),
                        new TypeReference<NettyResponse<Message<String>>>() {
                        });
                System.out.println("【receive】 Received response: " + response);
                return (Message<T>) response.getData();
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
    }

    /**
     * 取消订阅指定主题。
     *
     * @param topic    消息主题。
     * @param consumerId 消费者ID。
     */
    @Override
    public void unSubscribe(String topic, String consumerId) {
        executeWithClient(client -> {
            try {
                Map<String, String> params = Map.of("t", topic, "cid", consumerId);
                CompletableFuture<String> future = client.sendMessage("unsub", params, null);
                NettyResponse<String> response = JSON.parseObject(future.get(),
                        new TypeReference<NettyResponse<String>>() {
                        });
                System.out.println("【unsub】 Received response: " + response);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    /**
     * 确认消息消费，更新消费位点。
     *
     * @param topic    消息主题。
     * @param consumerId 消费者ID。
     * @param offset   消费位点。
     * @return 确认是否成功的布尔值。
     */
    @Override
    public Boolean ack(String topic, String consumerId, int offset) {
        return executeWithClient(client -> {
            try {
                Map<String, String> params = Map.of(
                        "t", topic,
                        "cid", consumerId,
                        "offset", String.valueOf(offset)
                );
                CompletableFuture<String> future = client.sendMessage("ack", params, null);
                NettyResponse<?> response = JSON.parseObject(future.get(),
                        new TypeReference<NettyResponse<String>>() {
                        });
                System.out.println("【ack】 Received response: " + response);
                return response.getCode() == 1;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        });
    }

    /**
     * 获取指定主题和消费者ID的消费统计信息。
     *
     * @param topic    消息主题。
     * @param consumerId 消费者ID。
     * @return 消费统计信息。
     */
    @Override
    public Statistical statistical(String topic, String consumerId) {
        return executeWithClient(client -> {
            try {
                Map<String, String> params = Map.of("t", topic, "cid", consumerId);
                CompletableFuture<String> future = client.sendMessage("stat", params, null);
                NettyResponse<Statistical> response = JSON.parseObject(future.get(),
                        new TypeReference<NettyResponse<Statistical>>() {
                        });
                System.out.println("【stat】 Received response: " + response);
                return response.getData();
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
    }
}
