package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yfy on 7/6/16.
 * PlatformTairThread.
 * Write tair periodly.
 */
public class PlatformTairThread implements Runnable {

  private ConcurrentHashMap<Long, PlatformData> map;

  private TairOperatorImpl tairOperator;

  public PlatformTairThread(ConcurrentHashMap<Long, PlatformData> map) {
    this.map = map;

    tairOperator = TairOperatorImpl.getRaceTairOperator();
//    tairOperator = TairOperatorImpl.getTestTairOperator();
  }

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(10000);
        for (long key : map.keySet()) {
          PlatformData data = map.get(key);
          tairOperator.write(RaceConfig.prex_taobao + key, data.getTaobao());
          tairOperator.write(RaceConfig.prex_tmall + key, data.getTmall());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
