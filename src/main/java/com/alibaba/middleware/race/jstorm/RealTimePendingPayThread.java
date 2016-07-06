package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/6/16.
 * RealTimePendingPayThread
 */
public class RealTimePendingPayThread implements Runnable {

  private LinkedBlockingQueue<PaymentMessage> payQueue;

  private RealTimePayThread realTimePayThread;

  public RealTimePendingPayThread(RealTimePayThread realTimePayThread) {
    payQueue = new LinkedBlockingQueue<>();
    this.realTimePayThread = realTimePayThread;
  }

  public void addPaymentMessage(PaymentMessage pm) {
    try {
      //RaceUtils.println("[RealTimePayBolt] addPendingPay " + pm.toString());
      payQueue.put(pm);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    while (true) {
      try {
        PaymentMessage pm = payQueue.take();
        realTimePayThread.dealPaymentMessage(pm);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
