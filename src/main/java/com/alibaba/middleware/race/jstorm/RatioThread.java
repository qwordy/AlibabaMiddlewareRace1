package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/10/16.
 * RatioThread
 */
public class RatioThread implements Runnable {

  RatioBolt bolt;

  private LinkedBlockingQueue<MyMessage> msgQueue;

  public RatioThread(RatioBolt bolt) {
    this.bolt = bolt;
  }

  public void addMsg(MyMessage msg) {
    msgQueue.offer(msg);
  }

  @Override
  public void run() {
    while (true) {
      try {
        MyMessage msg = msgQueue.take();
        bolt.deal(msg);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
