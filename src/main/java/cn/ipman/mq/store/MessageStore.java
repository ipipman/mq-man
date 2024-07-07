package cn.ipman.mq.store;

import cn.ipman.mq.model.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/7/6 20:56
 */
public class MessageStore {

    String topic;
    public static final int LEN = 1024 * 10; //10KB

    public MessageStore(String topic) {
        this.topic = topic;
    }

    MappedByteBuffer mappedByteBuffer = null;

    @SneakyThrows
    public void init() {
        File file = new File(this.topic + ".dat");
        if (!file.exists()) {
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        FileChannel channel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.READ, StandardOpenOption.WRITE);

        mappedByteBuffer = channel
                .map(FileChannel.MapMode.READ_WRITE, 0, LEN);


        // 初始化时判断topic文件中是否已经有了数据, 如果有的话需要将文件指针指向最新位置
        ByteBuffer buffer = mappedByteBuffer.asReadOnlyBuffer();
        byte[] header = new byte[10]; // 隐藏头,用来声明单个message的长度
        buffer.get(header);
        int offset = 0;
        while (header[9] > 0) {
            String trim = new String(header, StandardCharsets.UTF_8).trim();
            int len = Integer.parseInt(trim) + 10;
            System.out.println("store init topic = " + topic + ", len = " + len + ", header = " + trim);
            Indexer.addEntry(topic, offset, len); // 初始化历史数据
            // 计算下一个position,并尝试读取
            offset += len;
            System.out.println(" next = " + offset);
            buffer.position(offset);
            buffer.get(header);
        }
        buffer.clear();
        System.out.println("store init topic = " + topic + ", last position = " + offset);
        mappedByteBuffer.position(offset);
    }


    public int write(Message<String> message) {
        System.out.println("write position -> " + mappedByteBuffer.position()); // offset
        String msg = JSON.toJSONString(message);

        // 写入header头,方便store初始化时读取所有Message的索引信息
        int len = msg.getBytes(StandardCharsets.UTF_8).length;
        String format = String.format("%010d", len); // 用10个长度表示
        msg = format + msg;
        len = len + 10;

        // 写入数据
        int position = mappedByteBuffer.position(); // 获取当前偏移量
        Indexer.addEntry(this.topic, position, len);

        mappedByteBuffer.put(StandardCharsets.UTF_8.encode(msg));
        return position;
    }

    public int pos() {
        return mappedByteBuffer.position();
    }


    public Message<String> read(int offset) {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.getEntry(this.topic, offset);
        if (entry == null) return null;
        readOnlyBuffer.position(entry.getOffset() + 10); // 10是隐藏的header头

        int len = entry.getLength() - 10; // 数据的长度
        byte[] bytes = new byte[len];
        readOnlyBuffer.get(bytes, 0, len);
        String json = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("read only ==>> " + json);

        Message<String> message = JSON.parseObject(json, new TypeReference<Message<String>>() {
        });
        System.out.println("message.body = " + message);
        readOnlyBuffer.clear();
        return message;
    }

    public int total() {
        return Indexer.getEntries(topic).size();
    }

    public static void main(String[] args) {
        System.out.println(
                "{\"body\":\"{\\\"id\\\":0,\\\"item\\\":\\\"item0\\\",\\\"price\\\":0.0}\",\"headers\":{\"X-offset\":\"0\"},\"id\":0}"
                        .getBytes(StandardCharsets.UTF_8).length);

        String a = "{\"body\":\"{\\\"id\\\":0,\\\"item\\\":\\\"item0\\\",\\\"price\\\":0.0}\",\"headers\":{\"X-offset\":\"98\"},\"id\":0}";
        String b = "0000000090";
        String c = b + a;
        System.out.println(c);
        System.out.println(c.getBytes(StandardCharsets.UTF_8).length);

        System.out.println("=======> ");

        String e = String.format("%010d", 90);
        String d = e + a;
        System.out.println(d);
        System.out.println(d.getBytes(StandardCharsets.UTF_8).length);


    }
}
