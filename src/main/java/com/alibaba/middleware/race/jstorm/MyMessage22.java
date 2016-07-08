package com.alibaba.middleware.race.jstorm;

/**
 * Created by yfy on 7/5/16.
 * MyMessage22
 */
public class MyMessage22 {

  private String msgId, topic;

  private byte[] body;

  public MyMessage22() {}

  public MyMessage22(String msgId, String topic, byte[] body) {
    this.msgId = msgId;
    this.topic = topic;
    this.body = body;
  }

  public String getMsgId() {
    return msgId;
  }

  public String getTopic() {
    return topic;
  }

  public byte[] getBody() {
    return body;
  }
}
