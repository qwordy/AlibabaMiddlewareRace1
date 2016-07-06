package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/6/16.
 * RealTimePayThread
 */
public class RealTimePayThread implements Runnable {

  private LinkedBlockingQueue<PaymentMessage> payQueue;

  // history orders
  // orderId, myOrderMessage
  private ConcurrentHashMap<Long, MyOrderMessage> orderMap;

  // time, realTimePayData
  private ConcurrentHashMap<Long, RealTimePayData> resultMap;

  private final int ORDER_MAP_MAX_SIZE = 100000;

  public RealTimePayThread() {
    payQueue = new LinkedBlockingQueue<>(1000);
    orderMap = new ConcurrentHashMap<>();
  }

  public void addPaymentMessage(PaymentMessage pm) {
    try {
      payQueue.put(pm);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void addOrderMessage(OrderMessage om, String topic) {
    if (orderMap.size() < ORDER_MAP_MAX_SIZE) {
      short platform = topic.equals(RaceConfig.MqTaobaoTradeTopic) ?
          MyOrderMessage.TAOBAO : MyOrderMessage.TMALL;
      orderMap.put(om.getOrderId(), new MyOrderMessage(platform, om.getTotalPrice()));
    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        PaymentMessage pm = payQueue.take();
        long orderId = pm.getOrderId();
        MyOrderMessage om = orderMap.get(orderId);

        if (om != null) {
          double payAmount = pm.getPayAmount();
          long minuteTime = (pm.getCreateTime() / 1000 / 60) * 60;

          RealTimePayData data = resultMap.get(minuteTime);
          if (data == null)
            data = new RealTimePayData();

          if (om.platform == MyOrderMessage.TAOBAO)
            data.addTaobao(payAmount);
          else
            data.addTmall(payAmount);

          resultMap.put(minuteTime, data);

          if (om.minusPrice(payAmount))
            orderMap.remove(orderId);
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}