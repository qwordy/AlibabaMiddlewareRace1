package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yfy on 7/6/16.
 * PlatformThread
 */
public class PlatformThread implements Runnable {

  private LinkedBlockingQueue<PaymentMessage> payQueue;

  private final int MAX_SIZE = 5000000;

  // history orders
  // orderId, myOrderMessage
  private ConcurrentHashMap<Long, MyOrderMessage> orderMap;

  // time, realTimePayData
  private ConcurrentHashMap<Long, PlatformData> resultMap;

  private PlatformThread2 platformThread2;

  //private AtomicInteger payCount = new AtomicInteger();

  public PlatformThread() {
    payQueue = new LinkedBlockingQueue<>(MAX_SIZE);
    orderMap = new ConcurrentHashMap<>();
    resultMap = new ConcurrentHashMap<>();

    platformThread2 = new PlatformThread2(this);
    new Thread(platformThread2).start();

    new Thread(new PlatformTairThread(resultMap)).start();
  }

  public void addPaymentMessage(PaymentMessage pm) {
    try {
      payQueue.put(pm);
      //RaceUtils.println("[PlatformBolt] addPay " + pm.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void addOrderMessage(OrderMessage om, String topic) {
    while (orderMap.size() >= MAX_SIZE) {
      synchronized (orderMap) {
        try {
          //RaceUtils.println("[PlatformBolt] addOrderWait");
          orderMap.wait();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    //RaceUtils.println("[PlatformBolt] addOrderWaitEnd");
    short platform = topic.equals(RaceConfig.MqTaobaoTradeTopic) ?
        MyOrderMessage.TAOBAO : MyOrderMessage.TMALL;
    orderMap.put(om.getOrderId(), new MyOrderMessage(platform, om.getTotalPrice()));
    //RaceUtils.println("[PlatformBolt] addOrder " + om.toString());
  }

  public void dealPaymentMessage(PaymentMessage pm) {
//    try {
//      Thread.sleep(100);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    long orderId = pm.getOrderId();
    MyOrderMessage om = orderMap.get(orderId);

    if (om != null) {
      //System.out.println("[RealTime] payCount " + payCount.incrementAndGet());
      //RaceUtils.println("[PlatformBolt] dealPay " + pm.toString());
      double payAmount = pm.getPayAmount();
      long minuteTime = (pm.getCreateTime() / 1000 / 60) * 60;

      PlatformData data = resultMap.get(minuteTime);
      if (data == null)
        data = new PlatformData();

      if (om.platform == MyOrderMessage.TAOBAO)
        data.addTaobao(payAmount);
      else
        data.addTmall(payAmount);

      resultMap.put(minuteTime, data);
      //RaceUtils.println("[ReadTimePayBolt] put " + minuteTime + ' ' + data.toString());

      if (om.minusPrice(payAmount)) {
        orderMap.remove(orderId);
        synchronized (orderMap) {
          orderMap.notify();
        }
      }
    } else {
      platformThread2.addPaymentMessage(pm);
    }
  }

  @Override
  public void run() {
    while (true) {
      try {
        //RaceUtils.println("[PlatformThread] take pay");
        PaymentMessage pm = payQueue.take();
        dealPaymentMessage(pm);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
