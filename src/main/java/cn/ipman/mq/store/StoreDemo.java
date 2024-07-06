package cn.ipman.mq.store;

import cn.ipman.mq.model.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

/**
 * 内存映射, mmap store.
 *
 * @Author IpMan
 * @Date 2024/7/6 20:07
 */
public class StoreDemo {

    public static void main(String[] args) throws IOException {
        String content = """
                this is a good file.
                that is a new line for store.
                """;

        int length = content.getBytes(StandardCharsets.UTF_8).length;
        System.out.println(" len = " + length);
        File file = new File("test.dat");
        if (!file.exists()) {
            file.createNewFile();
        }

        Path path = Paths.get(file.getAbsolutePath());
        try (FileChannel channel =
                     (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            MappedByteBuffer mappedByteBuffer = channel
                    .map(FileChannel.MapMode.READ_WRITE, 0, 1024);

            for (int i = 0; i < 10; i++) {
                System.out.println(i + " -> " + mappedByteBuffer.position()); // offset
                @SuppressWarnings("unchecked")
                Message<String> message = (Message<String>) Message.createMessage(content, null);
                String msg = JSON.toJSONString(message);
                Indexer.addEntry("im.order", mappedByteBuffer.position(), msg.getBytes(StandardCharsets.UTF_8).length);
                mappedByteBuffer.put(StandardCharsets.UTF_8.encode(msg));
            }

            //length += 2;

            ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String line = sc.nextLine();
                if (line.equals("q")) break;
                System.out.println(" IN = " + line);

                int id = Integer.parseInt(line);

                Indexer.Entry entry = Indexer.getEntry("im.order", id);
                readOnlyBuffer.position(entry.getOffset());

                int len = entry.getLength();
                byte[] bytes = new byte[len];
                readOnlyBuffer.get(bytes, 0, len);

                String s = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("read only ==>> " + s);
                Message<String> message = JSON.parseObject(s, new TypeReference<Message<String>>() {
                });
                System.out.println(" message.body = " + message);

            }
        }
    }
}

