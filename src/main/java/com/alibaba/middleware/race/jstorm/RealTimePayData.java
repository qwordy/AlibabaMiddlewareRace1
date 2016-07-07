package com.alibaba.middleware.race.jstorm;

/**
 * Created by yfy on 7/6/16.
 * RealTimePayData
 */
public class RealTimePayData {

  private double taobao, tmall;

  public RealTimePayData(double taobao, double tmall) {
    this.taobao = taobao;
    this.tmall = tmall;
  }

  public RealTimePayData() {
    taobao = 0;
    tmall = 0;
  }

  public void addTaobao(double amount) {
    taobao += amount;
  }

  public void addTmall(double amount) {
    tmall += amount;
  }

  public Double getTaobao() {
    return taobao;
  }

  public Double getTmall() {
    return tmall;
  }

  @Override
  public String toString() {
    return taobao + " " + tmall;
  }
}
