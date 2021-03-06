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
//    objectId();
  }

  private static void objectId() {
    Test t1 = new Test();
    Test t2 = new Test();
    Test t3 = new Test();
    System.out.println(t1.hashCode());
    System.out.println(t2.hashCode());
    System.out.println(t3.hashCode());
  }

  private static void readTair() {
    tairOperator = TairOperatorImpl.getTestTairOperator();
    long ctime = (System.currentTimeMillis() / 1000 / 60) * 60;
    for (long time = 1468043700; time <= ctime; time += 60) {
      print(RaceConfig.prex_ratio + time);
      print(RaceConfig.prex_taobao + time);
      print(RaceConfig.prex_tmall + time);
    }
    System.out.println("Now: " + ctime);
  }

  private static void print(String key) {
    Double value = (Double) tairOperator.get(key);
    if (value != null)
      System.out.println(key + ' ' + value);
  }
}
