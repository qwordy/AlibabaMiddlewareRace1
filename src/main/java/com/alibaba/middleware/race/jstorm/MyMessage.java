package com.alibaba.middleware.race.jstorm;

/**
 * Created by yfy on 7/5/16.
 * MyMessage
 */
public class MyMessage {

  private String topic;

  private byte[] body;

  public MyMessage(String topic, byte[] body) {
    this.topic = topic;
    this.body = body;
  }

  public String getTopic() {
    return topic;
  }

  public byte[] getBody() {
    return body;
  }
}
