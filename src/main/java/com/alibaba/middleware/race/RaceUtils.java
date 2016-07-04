package com.alibaba.middleware.race;

import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.PrintWriter;


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

  private static PrintWriter pw;

  public static synchronized void printMsg(MessageExt msg, String head) {
    try {
      if (pw == null)
        pw = new PrintWriter("log");
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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
