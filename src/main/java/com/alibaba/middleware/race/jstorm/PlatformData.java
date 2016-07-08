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
