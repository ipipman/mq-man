package cn.ipman.mq.store;

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
                mappedByteBuffer.put(StandardCharsets.UTF_8.encode(i + ":" + content));
            }

            length += 2;

            ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String line = sc.nextLine();
                if (line.equals("q")) break;
                System.out.println(" IN = " + line);
                int pos = Integer.parseInt(line);
                readOnlyBuffer.position(pos * length);
                byte[] bytes = new byte[53];
                readOnlyBuffer.get(bytes, 0, length);
                System.out.println("read only ==>> " + new String(bytes, StandardCharsets.UTF_8));
            }
        }
    }
}

