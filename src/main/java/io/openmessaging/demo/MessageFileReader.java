package io.openmessaging.demo;

import io.openmessaging.BytesMessage;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * Created by Max on 2017/5/20.
 */
public class MessageFileReader {

    private MessageFileStore messageStore = MessageFileStore.getInstance();

    private File file;

    public byte[] read(String filePath, String bucket, MessageFileRecord record) {
        file = new File(filePath + File.separator + bucket);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            //todo 做一个读写池   写池再说，读池可以考虑
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(record.getRecord_pos());
            //StringBuffer  messageBody=new StringBuffer(messageFileRecordLenth);
            byte[] messageBody = new byte[record.getRecord_length()];
            raf.read(messageBody, 0, record.getRecord_length());
            raf.close();
            return messageBody;
            //System.out.println(new String(messageBody));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


    public static void main(String[] args) {
        MessageFileReader reader = new MessageFileReader();
        System.out.println(new String(reader.read("/Users/Max/code/tianchi", "QUEUE2", new MessageFileRecord(false, 294, 7))));
    }
}
