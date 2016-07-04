package com.alibaba.middleware.race.jstorm;

/**
 * Created by yfy on 7/4/16.
 * PayRatioData for one minute
 */
public class PayRatioData {

  public PayRatioData(double wireless, double pc) {
    w = wireless;
    p = pc;
  }

  public void addWireless(double amount) {
    w += amount;
  }

  public void addPc(double amount) {
    p += amount;
  }

  public double ratio() {
    return w / p;
  }

  @Override
  public String toString() {
    return String.format("wireless:%f pc:%f ratio:%f", w, p, w / p);
  }

  // wireless, pc
  private double w, p;

}
