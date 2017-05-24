package io.openmessaging.demo;


import io.openmessaging.BytesMessage;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Created by Max on 2017/5/19.
 */
public class MessageFileWriter {

    private File file;
    private long messageFileRecordStartPos;
    private int messageFileRecordLenth;
    private ByteBuffer byteBuffer;


    public MessageFileRecord write(boolean isTopic, String filePath, String bucket, BytesMessage message) {
        file = new File(filePath + File.separator + bucket);
        try {
            if (!file.exists()) {

                file.createNewFile();

            }
            //todo 做一个读写池   写池再说，读池可以考虑
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            messageFileRecordStartPos = raf.length();
            raf.seek(messageFileRecordStartPos);
            messageFileRecordLenth = message.getBody().length;
            raf.write(message.getBody(), 0, messageFileRecordLenth);
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new MessageFileRecord(isTopic, messageFileRecordStartPos, messageFileRecordLenth);
    }

    public void test() {


    }


}
