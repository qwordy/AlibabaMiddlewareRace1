package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/6/16.
 * RealTimePayThread
 */
public class RealTimePayThread implements Runnable {

  private BlockingQueue<PaymentMessage> payQueue;

  private ConcurrentHashMap<Long, MyOrderMessage> orderMap;

  public RealTimePayThread() {
    payQueue = new LinkedBlockingQueue<>(1000);
  }

  public void addPaymentMessage(PaymentMessage pm) {
    try {
      payQueue.put(pm);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void addOrderMessage(OrderMessage om) {

  }

  @Override
  public void run() {
    while (true) {

    }

  }
}
