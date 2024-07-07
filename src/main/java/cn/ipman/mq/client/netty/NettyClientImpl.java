package cn.ipman.mq.client.netty;

import cn.ipman.mq.client.ClientService;
import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.NettyResponse;
import cn.ipman.mq.model.Statistical;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyClientImpl implements ClientService {

    String host;
    int port;
    NettyMQClientPool clientPool;

    int maxTotal = 10;
    int maxIdle = 5;
    int minIdle = 2;

    public NettyClientImpl(String host, int port) {
        this.host = host;
        this.port = port;
        this.clientPool = new NettyMQClientPool(host, port, maxTotal, maxIdle, minIdle);
    }

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
