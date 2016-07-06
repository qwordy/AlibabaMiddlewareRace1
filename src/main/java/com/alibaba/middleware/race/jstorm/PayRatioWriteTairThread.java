package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.Map;

/**
 * Created by yfy on 7/6/16.
 * PayRatioWriteTairThread
 */
public class PayRatioWriteTairThread implements Runnable {

  private Map<Long, PayRatioData> map;

  private TairOperatorImpl tairOperator;

  public PayRatioWriteTairThread(Map<Long, PayRatioData> map) {
    this.map = map;

    tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
        RaceConfig.TairSalveConfigServer,
        RaceConfig.TairGroup, RaceConfig.TairNamespace);
  }

  @Override
  public void run() {
    try {
      Thread.sleep(30000);

      synchronized (map) {
        for (long key : map.keySet())
          tairOperator.write(RaceConfig.prex_ratio + key, map.get(key).ratio());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
