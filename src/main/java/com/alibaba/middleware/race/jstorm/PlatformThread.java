package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yfy on 7/6/16.
 * PlatformThread
 */
public class PlatformThread implements Runnable {

//  private final int MAX_SIZE = 5000000;

  private LinkedBlockingQueue<PaymentMessage> payQueue;

  // shared with platformBolt
  private ConcurrentHashMap<Long, MyOrderMessage> orderMap;

  // shared with platformBolt
  private ConcurrentHashMap<Long, PlatformData> resultMap;

  // shared with platformBolt
  private byte[] lock;

  public PlatformThread(ConcurrentHashMap<Long, MyOrderMessage> orderMap,
                        ConcurrentHashMap<Long, PlatformData> resultMap,
                        byte[] lock) {
    payQueue = new LinkedBlockingQueue<>();
    this.orderMap = orderMap;
    this.resultMap = resultMap;
    this.lock = lock;
  }

  public void addPay(PaymentMessage pm) {
    payQueue.offer(pm);
  }

  // Wait until orderId is in orderMap
  public void dealPay(PaymentMessage pm) {
    long orderId = pm.getOrderId();
    MyOrderMessage om = orderMap.get(orderId);
    while (om == null) {
      try {
        Thread.sleep(1);
      } catch (Exception e) {
        e.printStackTrace();
      }
      om = orderMap.get(orderId);
    }

    double payAmount = pm.getPayAmount();
    long minuteTime = (pm.getCreateTime() / 1000 / 60) * 60;

    PlatformData data;
    synchronized (lock) {
      data = resultMap.get(minuteTime);
      if (data == null) {
        data = new PlatformData();
        resultMap.put(minuteTime, data);
      }
    }

    if (om.taobao())
      data.addTaobao(payAmount);
    else
      data.addTmall(payAmount);

    if (om.minusPrice(payAmount))
      orderMap.remove(orderId);
  }

  @Override
  public void run() {
    while (true) {
      try {
        PaymentMessage pm = payQueue.take();
        dealPay(pm);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
