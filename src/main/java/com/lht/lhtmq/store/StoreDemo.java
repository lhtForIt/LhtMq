package com.lht.lhtmq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.lht.lhtmq.model.LhtMessage;

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
import java.util.List;
import java.util.Scanner;

/**
 * @author Leo
 * @date 2024/07/06
 */
public class StoreDemo {
    public static void main(String[] args) throws IOException {
        String content = """
            this is a test file.
            that is a new line for store.
            """;

        int length = content.getBytes().length;
        System.out.println(" len= " + length);
        File file = new File("test.dat");

        if (!file.exists()) file.createNewFile();

        Path path = Paths.get(file.getAbsolutePath());

        try (FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            //映射一定大小的缓冲区，我们后面对缓冲区的操作操作系统都会同步到文件
            MappedByteBuffer mappedByteBuffer = channel
                    .map(FileChannel.MapMode.READ_WRITE, 0, 1024);
            for (int i = 0; i < 10; i++) {
                System.out.println(i + " -> " + mappedByteBuffer.position());
                LhtMessage<String> message = LhtMessage.create(i + ":" + content, null);
                String msg = JSON.toJSONString(message);
                Indexer.addEntry("test", mappedByteBuffer.position(), msg.getBytes(StandardCharsets.UTF_8).length);
                mappedByteBuffer.put(Charset.forName("UTF-8").encode(msg));//对字符编码写入，防止乱码
            }

            length += 2;

            ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);

            while (sc.hasNext()){
                String s = sc.nextLine();
                if (s.equals("q")) break;
                System.out.println(" IN = " + s);
                int pos = Integer.parseInt(s);
                Indexer.Entry entry = Indexer.getEntry("test", pos);
                readOnlyBuffer.position(entry.getOffset());
                int len = entry.getLength();
                byte[] bytes = new byte[len];
                readOnlyBuffer.get(bytes, 0, len);
                String ss = new String(bytes, Charset.forName("UTF-8"));
                System.out.println(" read only ==>> "+ ss);
                LhtMessage<String> message = JSON.parseObject(ss, new TypeReference<LhtMessage<String>>() {});
                System.out.println(" message.body ==> " + message.getBody());
            }


        }



    }




}
