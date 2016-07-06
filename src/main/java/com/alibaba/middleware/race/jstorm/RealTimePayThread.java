package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/6/16.
 * RealTimePayThread
 */
public class RealTimePayThread implements Runnable {

  private LinkedBlockingQueue<PaymentMessage> payQueue;

  private final int MAX_SIZE = 100000;

  // history orders
  // orderId, myOrderMessage
  private ConcurrentHashMap<Long, MyOrderMessage> orderMap;

  // time, realTimePayData
  private ConcurrentHashMap<Long, RealTimePayData> resultMap;

  private RealTimePendingPayThread realTimePendingPayThread;

  public RealTimePayThread() {
    payQueue = new LinkedBlockingQueue<>(MAX_SIZE);
    orderMap = new ConcurrentHashMap<>();
    resultMap = new ConcurrentHashMap<>();

    realTimePendingPayThread = new RealTimePendingPayThread(this);
    new Thread(realTimePendingPayThread).start();

    new Thread(new RealTimePayWriteTairThread(resultMap)).start();
  }

  public void addPaymentMessage(PaymentMessage pm) {
    try {
      payQueue.put(pm);
      //RaceUtils.println("[RealTimePayBolt] addPay " + pm.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void addOrderMessage(OrderMessage om, String topic) {
    while (orderMap.size() >= MAX_SIZE);
    //RaceUtils.println("[RealTimePayBolt] addOrder " + om.toString());
    short platform = topic.equals(RaceConfig.MqTaobaoTradeTopic) ?
        MyOrderMessage.TAOBAO : MyOrderMessage.TMALL;
    orderMap.put(om.getOrderId(), new MyOrderMessage(platform, om.getTotalPrice()));
  }

  public void dealPaymentMessage(PaymentMessage pm) {
    //RaceUtils.println("[RealTimePayBolt] dealPay " + pm.toString());

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
      RaceUtils.println("[ReadTimePayBolt] put " + minuteTime + ' ' + data.toString());

      if (om.minusPrice(payAmount))
        orderMap.remove(orderId);
    } else {
      realTimePendingPayThread.addPaymentMessage(pm);
    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        //RaceUtils.println("[RealTimePayThread] take pay");
        PaymentMessage pm = payQueue.take();
        //RaceUtils.println("[RealTimePayThread] take pay success " + pm.toString());
        dealPaymentMessage(pm);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
