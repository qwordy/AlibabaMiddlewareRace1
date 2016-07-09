package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/6/16.
 * RatioTairThread
 */
public class RatioTairThread implements Runnable {

  // latest data
  private Map<Long, RatioData> map;

  // last sync data
  private Map<Long, Double> syncMap;

  private TairOperatorImpl tairOperator;

  public RatioTairThread(Map<Long, RatioData> map) {
    this.map = map;
    syncMap = new HashMap<>();

    tairOperator = TairOperatorImpl.getRaceTairOperator();
//    tairOperator = TairOperatorImpl.getTestTairOperator();
  }

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(10000);
      } catch (Exception e) {
        e.printStackTrace();
      }

      for (long key : map.keySet()) {
        double ratio = map.get(key).ratio();
        Double ratioOld = syncMap.get(key);
        if (ratioOld == null || ratio != ratioOld) {
          tairOperator.write(RaceConfig.prex_ratio + key, ratio);
          syncMap.put(key, ratio);
        }
      }
    }
  }
}
