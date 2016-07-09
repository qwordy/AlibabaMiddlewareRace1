package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.MyMessage;

/**
 * Created by yfy on 7/8/16
 * Test
 */
public class Test {

  private static TairOperatorImpl tairOperator;

  public static void main(String[] args) {
    readTair();
  }

  private static void readTair() {
    tairOperator = TairOperatorImpl.getTestTairOperator();
    long ctime = (System.currentTimeMillis() / 1000 / 60) * 60;
    System.out.println("Now: " + ctime);
    for (long time = 1468043700; time <= ctime; time += 60) {
      print(RaceConfig.prex_ratio + time);
      print(RaceConfig.prex_taobao + time);
      print(RaceConfig.prex_tmall + time);
    }
  }

  private static void print(String key) {
    Double value = (Double) tairOperator.get(key);
    if (value != null)
      System.out.println(key + ' ' + value);
  }
}
