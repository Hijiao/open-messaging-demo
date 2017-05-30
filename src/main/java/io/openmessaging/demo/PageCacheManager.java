package io.openmessaging.demo;

import io.openmessaging.MessageHeader;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Map;
import java.util.Set;

import static io.openmessaging.demo.Constants.*;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheManager {

    //TODO 批量移动buffer http://www.cnblogs.com/lxzh/archive/2013/05/10/3071680.html

    private String storePath;
    private String bucket;

    public PageCacheManager(String bucket, String storePath) {
        this.bucket = bucket;
        this.storePath = storePath;
    }

    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 300);//消息大小最大256kb


    private int currPageNumber = -1;
    private MappedByteBuffer currPage;
    int currPageRemaining;


    String key;
    String value;
    byte[] body;
    Set<Map.Entry<String, Object>> entrySet;

    public void writeMessage(DefaultBytesMessage message) {
        byteBuffer.clear();
        if (currPage == null) {
            currPage = createNewPageToWrite(++currPageNumber);
        }
        entrySet = ((DefaultKeyValue) message.headers()).getMap().entrySet();
        if (entrySet.size() != 1) {
            for (Map.Entry<String, Object> kv : entrySet) {
                key = kv.getKey();
                if ((!MessageHeader.TOPIC.equals(key)) && (!MessageHeader.QUEUE.equals(key))) {//MessageId
                    byteBuffer.put(key.getBytes());
                    byteBuffer.put(MARKET_PREFIX_HEADER_KEY);
                    byteBuffer.put(((String) kv.getValue()).getBytes());
                    byteBuffer.put(MARKET_PREFIX_HEADER_VALUE);
                }
            }
        }

        DefaultKeyValue properties = (DefaultKeyValue) message.properties();

        if (properties != null) {
            entrySet = properties.getMap().entrySet();
            for (Map.Entry<String, Object> kv : entrySet) {
                key = kv.getKey();
                byteBuffer.put(key.getBytes());
                byteBuffer.put(MARKET_PREFIX_PRO_KEY);
                byteBuffer.put(((String) kv.getValue()).getBytes());
                byteBuffer.put(MARKET_PREFIX_PRO_VALUE);
            }
        }

        byteBuffer.put(message.getBody());
        byteBuffer.put(MARKET_PREFIX_MESSAGE_END);

        currPageRemaining = currPage.remaining();
        int messageLen = byteBuffer.position();
        byteBuffer.rewind();
        if (messageLen >= currPageRemaining) {
            for (int i = 0; i < currPageRemaining; i++) {
                currPage.put(byteBuffer.get());
            }
            closeCurrPage();
            currPage = createNewPageToWrite(++currPageNumber);
            for (int i = currPageRemaining; i < messageLen; i++) {
                currPage.put(byteBuffer.get());
            }
        } else {
            for (int i = 0; i < messageLen; i++) {
                currPage.put(byteBuffer.get());
            }
        }

    }


    private byte getNextByteFromCurrPage() {

        if (currPage != null) {
            if (currPage.hasRemaining()) {
                return currPage.get();
            } else {
                closeCurrPage();
                currPage = createNewPageToRead(++currPageNumber);
            }
        }
        if (currPage == null)
            return MARKER_PREFIX;//连着返回两次，则认为文件读取结束
        return currPage.get();
    }

    boolean hasPackagedOneMessage = false;
    boolean finishFlag = false;

    public DefaultBytesMessage readMessage() {
        hasPackagedOneMessage = false;

        if (currPage == null && currPageNumber == -1) {
            currPage = createNewPageToRead(++currPageNumber);
        }
        if (currPage == null) {
            return null;
        }
        byteBuffer.clear();
        currPageRemaining = currPage.remaining();
        byte currByte;
        byte[] keyBytes;


        DefaultBytesMessage message = new DefaultBytesMessage();

        while ((!hasPackagedOneMessage) && (!finishFlag)) {
            currByte = getNextByteFromCurrPage();
            if (currByte != MARKER_PREFIX) {
                if (currByte == 0x00) {
                    return null;
                }
                byteBuffer.put(currByte);
            } else {
                switch (getNextByteFromCurrPage()) {
                    case HEADER_KEY_END_MARKER:
                        keyBytes = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(keyBytes);
                        key = new String(keyBytes);
                        break;
                    case HEADER_VALUE_END_MARKER:
                        keyBytes = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(keyBytes);
                        value = new String(keyBytes);
                        message.putHeaders(key, value);
                        break;
                    case PRO_KEY_END_MARKER:
                        keyBytes = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(keyBytes);
                        key = new String(keyBytes);
                        break;
                    case PRO_VALUE_END_MARKER:
                        keyBytes = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(keyBytes);
                        value = new String(keyBytes);
                        message.putProperties(key, value);
                        break;
                    case MESSAGE_END_MARKER:
                        body = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(body);
                        message.setBody(body);
                        hasPackagedOneMessage = true;
                        break;
                    case MARKER_PREFIX:
                        finishFlag = true;
                        break;
                }
                byteBuffer.clear();
            }
        }
        if (hasPackagedOneMessage) {
            return message;
        }
        return null;
    }

    public void writeByte(byte[] body) {

        if (currPage == null) {
            currPage = createNewPageToWrite(++currPageNumber);
        }
        currPageRemaining = currPage.remaining();
        int bodyAndIndexLenth = body.length + 4;
        if (currPageRemaining < bodyAndIndexLenth) {
            byte[] bodyAndIndex = packageBody(body, bodyAndIndexLenth);
            for (int i = 0; i < currPageRemaining; i++) {
                currPage.put(bodyAndIndex[i]);
            }
            closeCurrPage();
            currPage = createNewPageToWrite(++currPageNumber);
            for (int i = currPageRemaining; i < bodyAndIndexLenth; i++) {
                currPage.put(bodyAndIndex[i]);
            }
            //for (int i =0;i<len;++i)
            //tarByteArray=currPage.get();
        } else {
            byte[] index = intToByteArray(body.length);
            currPage.put(index[0]);
            currPage.put(index[1]);
            currPage.put(index[2]);
            currPage.put(index[3]);
            for (int i = 0; i < body.length; i++) {
                currPage.put(body[i]);
            }
        }

    }

    byte[] indexByte = new byte[4];

    public byte[] readByte() {
        if (currPage == null && currPageNumber == -1) {
            currPage = createNewPageToRead(++currPageNumber);
        }
        if (currPage == null) {
            return null;
        }
        currPageRemaining = currPage.remaining();

        if (currPageRemaining <= 4) {
            for (int i = 0; i < currPageRemaining; i++)
                indexByte[i] = currPage.get();
            currPage = createNewPageToRead(++currPageNumber);
            if (currPage == null) {
                return null;
            }
            for (int i = currPageRemaining; i < 4; i++) {
                indexByte[i] = currPage.get();
            }
        } else {
            for (int i = 0; i < 4; i++) {
                indexByte[i] = currPage.get();
            }
        }

        int messageLen = byteArrayToInt(indexByte);
        if (messageLen == 0) {
            return null;
        }
        byte[] body = new byte[messageLen];
        currPageRemaining = currPage.remaining();
        if (currPageRemaining < messageLen) {
            for (int i = 0; i < currPageRemaining; i++) {
                body[i] = currPage.get();
            }
            currPage = createNewPageToRead(++currPageNumber);
            if (currPage == null) {
                return null;
            }
            for (int l = currPageRemaining; l < messageLen; l++) {
                body[l] = currPage.get();
            }
        } else {
            for (int l = 0; l < messageLen; l++) {
                body[l] = currPage.get();
            }
        }
        return body;
    }


    private MappedByteBuffer createNewPageToWrite(int index) {
        StringBuilder builder = new StringBuilder();
        builder.append(storePath).append(File.separator).append(bucket).append("_").append(String.format("%03d", index));
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(new File(builder.toString()), "rw");
            MappedByteBuffer newPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Constants.PAGE_SIZE);
            return newPage;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private MappedByteBuffer createNewPageToRead(int index) {
        StringBuilder builder = new StringBuilder();
        builder.append(storePath).append(File.separator).append(bucket).append("_").append(String.format("%03d", index));
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(new File(builder.toString()), "r");
            MappedByteBuffer newPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, Constants.PAGE_SIZE);
            return newPage;
        } catch (Exception e) {
            //e.printStackTrace();
            return null;
        }
    }


    public void closeCurrPage() {
        try {
//            MappedByteBuffer page = bucketPageList.getLast();
//            page.force();
            Method getCleanerMethod = currPage.getClass().getMethod("cleaner", new Class[0]);
            getCleanerMethod.setAccessible(true);
            sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(currPage, new Object[0]);
            cleaner.clean();
            System.gc();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int byteArrayToInt(byte[] b) {
        return b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public static byte[] packageBody(byte[] body, int bodyAndIndexLenth) {
        byte[] newBody = new byte[bodyAndIndexLenth];
        int a = body.length;
        newBody[0] = (byte) ((a >> 24) & 0xFF);
        newBody[1] = (byte) ((a >> 16) & 0xFF);
        newBody[2] = (byte) ((a >> 8) & 0xFF);

        newBody[3] = (byte) (a & 0xFF);
        for (int i = 0; i < a; a++) {
            newBody[i + 4] = body[i];
        }
        return newBody;

    }

    private static String decoder(ByteBuffer buffer) {
        Charset charset = Charset.forName("US-ASCII");
        CharsetDecoder decoder = charset.newDecoder();
        try {
            return decoder.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        String s = "abcdef";
        byte[] bytes = new byte[]{'a', 'b', 'c', 'd', 'e', 0};
        byteBuffer.put(s.getBytes(Charset.forName("US-ASCII")));
        byte[] out = new byte[6];
        System.out.println("byteBuffer pos:" + byteBuffer.position());
        System.out.println("byteBuffer remaining Size:" + byteBuffer.remaining());
        // byteBuffer.flip();
        System.out.println("byteBuffer pos:" + byteBuffer.position());

        System.out.println("byteBuffer  remaining Size:" + byteBuffer.remaining());

//        StringBuilder
//        StringBuilder builder=new StringBuilder("UTF-8");
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < byteBuffer.position(); i++) {
            buffer.append(byteBuffer.get(i));
        }


//        System.out.println(new String(out));
//        String b=decoder(byteBuffer);
//        System.out.println(b);
        System.out.println(buffer.toString());
        byte nullb = byteBuffer.get(7);
        System.out.println(nullb == 0);
        System.out.println();


    }

}


