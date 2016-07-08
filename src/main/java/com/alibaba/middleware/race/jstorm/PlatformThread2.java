package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/6/16.
 * PlatformThread2
 */
public class PlatformThread2 implements Runnable {

  private LinkedBlockingQueue<PaymentMessage> payQueue;

  private PlatformThread platformThread;

  public PlatformThread2(PlatformThread platformThread) {
    payQueue = new LinkedBlockingQueue<>();
    this.platformThread = platformThread;
  }

  public void addPaymentMessage(PaymentMessage pm) {
    try {
      //RaceUtils.println("[PlatformBolt] addPendingPay " + pm.toString());
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
        platformThread.dealPaymentMessage(pm);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
