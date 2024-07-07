package cn.ipman.mq.demo;

import cn.ipman.mq.client.netty.NettyMQClient;
import cn.ipman.mq.client.netty.NettyMQClientPool;
import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.NettyResponse;
import cn.ipman.mq.model.Statistical;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyClientDemo {


    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 6666;
        int maxTotal = 10;
        int maxIdle = 5;
        int minIdle = 2;

        NettyMQClientPool clientPool = new NettyMQClientPool(host, port, maxTotal, maxIdle, minIdle);
        try {
            // 借用客户端
            NettyMQClient client = clientPool.borrowClient();

            sub(client);
            statistical(client);

            for (int i = 0; i < 10; i++) {
                send(client);
            }

            NettyResponse<Message<String>> receive = receive(client);
            assert receive != null;
            int offset = Integer.parseInt(receive.getData().getHeaders().get("X-offset"));
            ack(client, offset);

            NettyResponse<List<Message<String>>> batchReceive = batchReceive(client, 50);
            assert batchReceive != null;
            int maxOffset = batchReceive.getData().stream()
                    .mapToInt(msg -> Integer.parseInt(msg.getHeaders().get("X-offset")))
                    .max()
                    .orElse(0);
            long totalOffset = batchReceive.getData().size();
            System.out.println("batch receive total/max = " + totalOffset + "/" + maxOffset);
            ack(client, maxOffset);

            statistical(client);

            unsub(client);

            // 归还客户端
            clientPool.returnClient(client);
        } finally {
            clientPool.close();
        }
    }

    public static void sub(NettyMQClient client) {
        Map<String, String> params = Map.of(
                "t", "cn.ipman.test",
                "cid", "123");
        // 发送消息
        CompletableFuture<String> future = client.sendMessage("sub", params, null);
        try {
            NettyResponse<String> response = JSON.parseObject(future.get(),
                    new TypeReference<NettyResponse<String>>() {
                    });
            System.out.println("【sub】 Received response: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void unsub(NettyMQClient client) {
        Map<String, String> params = Map.of(
                "t", "cn.ipman.test",
                "cid", "123");
        // 发送消息
        CompletableFuture<String> future = client.sendMessage("unsub", params, null);
        try {
            NettyResponse<String> response = JSON.parseObject(future.get(),
                    new TypeReference<NettyResponse<String>>() {
                    });
            System.out.println("【unsub】 Received response: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void send(NettyMQClient client) {
        Map<String, String> params = Map.of(
                "t", "cn.ipman.test",
                "cid", "123");
        Order order = new Order(1, "item" + 1, 100);
        Message<String> message = new Message<>(1, JSON.toJSONString(order), null);

        // 发送消息
        CompletableFuture<String> future = client.sendMessage("send", params, message);
        try {
            NettyResponse<String> response = JSON.parseObject(future.get(),
                    new TypeReference<NettyResponse<String>>() {
                    });
            System.out.println("【send】 Received response: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static NettyResponse<Message<String>> receive(NettyMQClient client) {
        Map<String, String> params = Map.of(
                "t", "cn.ipman.test",
                "cid", "123");
        // 发送消息
        CompletableFuture<String> future = client.sendMessage("receive", params, null);
        try {
            NettyResponse<Message<String>> response = JSON.parseObject(future.get(),
                    new TypeReference<NettyResponse<Message<String>>>() {
                    });
            System.out.println("【batch receive】 Received response: " + response);
            return response;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static NettyResponse<List<Message<String>>> batchReceive(NettyMQClient client, int size) {
        Map<String, String> params = Map.of(
                "t", "cn.ipman.test",
                "cid", "123",
                "size", String.valueOf(size));
        // 发送消息
        CompletableFuture<String> future = client.sendMessage("batch-receive", params, null);
        try {
            NettyResponse<List<Message<String>>> response = JSON.parseObject(future.get(),
                    new TypeReference<NettyResponse<List<Message<String>>>>() {
                    });
            System.out.println("【receive】 Received response: " + response);
            return response;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void ack(NettyMQClient client, int ack) {
        Map<String, String> params = Map.of(
                "t", "cn.ipman.test",
                "cid", "123",
                "offset", String.valueOf(ack));

        // 发送消息
        CompletableFuture<String> future = client.sendMessage("ack", params, null);
        try {
            NettyResponse<?> response = JSON.parseObject(future.get(),
                    new TypeReference<NettyResponse<String>>() {
                    });
            System.out.println("【ack】 Received response: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static NettyResponse<Statistical> statistical(NettyMQClient client) {
        Map<String, String> params = Map.of("t", "cn.ipman.test", "cid", "123");
        // 发送消息
        CompletableFuture<String> future = client.sendMessage("stat", params, null);
        try {
            NettyResponse<Statistical> response = JSON.parseObject(future.get(),
                    new TypeReference<NettyResponse<Statistical>>() {
                    });
            System.out.println("【stat】 Received response: " + response);
            return response;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
