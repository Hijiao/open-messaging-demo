package io.openmessaging.demo;

import io.openmessaging.BytesMessage;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnit {
    private MessageFileRecord record;
    private BytesMessage message;


    public PageCacheWriteUnit(MessageFileRecord re, BytesMessage message) {
        this.record = re;
        this.message = message;
    }

    public MessageFileRecord getRecord() {
        return record;
    }

    public void setRecord(MessageFileRecord record) {
        this.record = record;
    }

    public BytesMessage getMessage() {
        return message;
    }

    public void setMessage(BytesMessage message) {
        this.message = message;
    }
}
