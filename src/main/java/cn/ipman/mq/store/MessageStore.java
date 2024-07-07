package cn.ipman.mq.store;

import cn.ipman.mq.model.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.SneakyThrows;
import org.apache.catalina.Store;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/7/6 20:56
 */
public class MessageStore {

    String topic;
    public static final int LEN = 1024 * 1024; //1MB

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

        // todo 1、读取索引
        // TODO: 判断是否有数据,找到数据结尾,  通过 mappedByteBuffer.position() 获取当前偏移量
        // 读前10位,转成int=len, 看是不是大于0,往后翻len的长度,就是下一条记录,
        // 重复上一步, 一直到0为止,找到数据的结尾
        //  mappedByteBuffer.position(init_pos)

        // todo > 10M
        // 需要创建第二个数据文件,怎么来管理

    }


    public int write(Message<String> message) {
        System.out.println("write position -> " + mappedByteBuffer.position()); // offset
        String msg = JSON.toJSONString(message);

        // 1000_1000_10
        int len = msg.getBytes(StandardCharsets.UTF_8).length;

        // todo:
//        String format = String.format("%010d", len);
//        msg = format + msg;
//        len += 10;

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
        readOnlyBuffer.position(entry.getOffset());

        int len = entry.getLength(); // 数据的长度
        byte[] bytes = new byte[len];
        readOnlyBuffer.get(bytes, 0, len);
        String json = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("read only ==>> " + json);

        Message<String> message = JSON.parseObject(json, new TypeReference<Message<String>>() {
        });
        System.out.println("message.body = " + message);
        return message;
    }

    public int total() {
        return Indexer.getEntries(topic).size();
    }

}
