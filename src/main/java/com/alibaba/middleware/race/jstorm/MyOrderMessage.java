package com.alibaba.middleware.race.jstorm;

/**
 * Created by yfy on 7/6/16.
 * MyOrderMessage
 */
public class MyOrderMessage {

  public static final short TAOBAO = 0;

  public static final short TMALL = 1;

  //public long orderId;

  public short platform;

  public double price, hasPayedPrice;

  public MyOrderMessage(short platform, double price) {
    //this.orderId = orderId;
    this.platform = platform;
    this.price = price;
    hasPayedPrice = 0;
  }
}
