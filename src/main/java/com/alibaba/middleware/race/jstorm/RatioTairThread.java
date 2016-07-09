package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.Map;

/**
 * Created by yfy on 7/6/16.
 * RatioTairThread
 */
public class RatioTairThread implements Runnable {

  private Map<Long, RatioData> map;

  private TairOperatorImpl tairOperator;

  public RatioTairThread(Map<Long, RatioData> map) {
    this.map = map;

    tairOperator = TairOperatorImpl.getRaceTairOperator();
//    tairOperator = TairOperatorImpl.getTestTairOperator();
  }

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(10000);
        for (long key : map.keySet())
          tairOperator.write(RaceConfig.prex_ratio + key, map.get(key).ratio());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
