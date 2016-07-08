package com.alibaba.middleware.race;

import com.alibaba.middleware.race.jstorm.MyMessage;
import com.alibaba.middleware.race.jstorm.RatioData;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.PrintWriter;
import java.util.Map;


public class RaceUtils {
  /**
   * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
   * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
   *
   * @param object
   * @return
   */
  public static byte[] writeKryoObject(Object object) {
    Output output = new Output(1024);
    Kryo kryo = new Kryo();
    kryo.writeObject(output, object);
    output.flush();
    output.close();
    byte[] ret = output.toBytes();
    output.clear();
    return ret;
  }

  public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
    Kryo kryo = new Kryo();
    Input input = new Input(bytes);
    input.close();
    T ret = kryo.readObject(input, tClass);
    return ret;
  }

  public static PrintWriter pw;

  public static void initLog() {
    try {
      pw = new PrintWriter("log");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static synchronized void printMsg(MyMessage msg, String head) {
    pw.print(head + ' ');
    byte[] body = msg.getBody();
    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
      pw.println("End of " + msg.getTopic());
    } else if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
      PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
      pw.println(paymentMessage);
    } else {
      OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
      pw.println(orderMessage);
    }
    pw.flush();
  }

  public static synchronized void printRatio(Map<Long, RatioData> map) {
    pw.println("[ratio] map size " + map.size());
    for (long key : map.keySet()) {
      RatioData value = map.get(key);
      pw.printf("[ratio] key:%d %s\n", key, value);
    }
    pw.flush();
  }

  public static synchronized void println(Object o) {
    pw.println(o + " " + System.currentTimeMillis());
    pw.flush();
  }
}
