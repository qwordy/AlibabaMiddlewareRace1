package com.alibaba.middleware.race.jstorm;

/**
 * Created by yfy on 7/6/16.
 * PlatformData
 */
public class PlatformData {

  private double taobao, tmall;

  public PlatformData(double taobao, double tmall) {
    this.taobao = taobao;
    this.tmall = tmall;
  }

  public PlatformData() {
    taobao = 0;
    tmall = 0;
  }

  public synchronized void addTaobao(double amount) {
    taobao += amount;
  }

  public synchronized void addTmall(double amount) {
    tmall += amount;
  }

  public double getTaobao() {
    return taobao;
  }

  public double getTmall() {
    return tmall;
  }

  @Override
  public String toString() {
    return taobao + " " + tmall;
  }
}
