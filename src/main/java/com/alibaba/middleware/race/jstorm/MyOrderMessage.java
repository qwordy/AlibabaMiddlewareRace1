package com.alibaba.middleware.race.jstorm;

/**
 * Created by yfy on 7/6/16.
 * MyOrderMessage
 */
public class MyOrderMessage {

  public static final short TAOBAO = 0;

  public static final short TMALL = 1;

  //public long orderId;

  private short platform;

  private double remainPrice;

  public MyOrderMessage(short platform, double price) {
    //this.orderId = orderId;
    this.platform = platform;
    this.remainPrice = price;
  }

  public boolean taobao() {
    return platform == TAOBAO;
  }

  public synchronized boolean minusPrice(double price) {
    remainPrice -= price;
    return remainPrice <= 0;
  }
}
