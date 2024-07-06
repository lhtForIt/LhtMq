package com.lht.lhtmq.store;

import ch.qos.logback.core.joran.conditional.IfAction;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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


        File file = new File("test.dat");

        if (!file.exists()) file.createNewFile();

        Path path = Paths.get(file.getAbsolutePath());

        try (FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024);
            for (int i = 0; i < 10; i++) {
                mappedByteBuffer.put(Charset.forName("UTF-8").encode(content));
            }

        }



    }




}
