package com.lht.lhtmq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.lht.lhtmq.model.LhtMessage;
import lombok.SneakyThrows;

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

/**
 * message store class
 *
 * @author Leo
 * @date 2024/07/19
 */
public class MessageStore {

    public static final int LEN = 1024 * 1024;
    MappedByteBuffer mappedByteBuffer;

    String topic;

    public MessageStore(String topic) {
        this.topic = topic;
    }

    @SneakyThrows
    public void init() {
        File file = new File(topic + ".dat");

        if (!file.exists()) file.createNewFile();

        Path path = Paths.get(file.getAbsolutePath());

        FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        mappedByteBuffer = channel
                .map(FileChannel.MapMode.READ_WRITE, 0, LEN);//这个空间在rocket mq里面默认是100m
    }

    public int write(LhtMessage<String> message) {
        System.out.println(" write pos -> " + mappedByteBuffer.position());
        String msg = JSON.toJSONString(message);
        int position = mappedByteBuffer.position();
        Indexer.addEntry("test", position, msg.getBytes(StandardCharsets.UTF_8).length);
        mappedByteBuffer.put(Charset.forName("UTF-8").encode(msg));//对字符编码写入，防止乱码
        return position;
    }

    public int pos(){
        return mappedByteBuffer.position();
    }

    public LhtMessage<String> read(int offset) {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.getEntry("test", offset);
        readOnlyBuffer.position(entry.getOffset());
        int len = entry.getLength();
        byte[] bytes = new byte[len];
        readOnlyBuffer.get(bytes, 0, len);
        String json = new String(bytes, Charset.forName("UTF-8"));
        System.out.println(" read json ==>> " + json);
        LhtMessage<String> message = JSON.parseObject(json, new TypeReference<LhtMessage<String>>() {});
        System.out.println(" message.body ==> " + message.getBody());
        return message;
    }

}
