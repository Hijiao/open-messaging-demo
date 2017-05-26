package io.openmessaging.demo;

import org.omg.PortableServer.THREAD_POLICY_ID;

/**
 * Created by Max on 2017/5/19.
 */
public class MessageFileRecord {
    private long record_pos;
    private int record_length;

    public MessageFileRecord(long record_pos, int record_length) {
        this.record_pos = record_pos;
        this.record_length = record_length;
    }

    public long getRecord_pos() {
        return record_pos;
    }

    public void setRecord_pos(long record_pos) {
        this.record_pos = record_pos;
    }

    public int getRecord_length() {
        return record_length;
    }

    public void setRecord_length(int record_length) {
        this.record_length = record_length;
    }


    @Override
    public String toString() {
        return "record_pos=" + record_pos +
                ", record_length=" + record_length +
                '}';
    }
}
