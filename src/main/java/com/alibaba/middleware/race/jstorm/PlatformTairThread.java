package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/6/16.
 * PlatformTairThread.
 * Write tair periodly.
 */
public class PlatformTairThread implements Runnable {

  private Map<Long, PlatformData> map;

  private Map<Long, PlatformData> syncMap;

  private TairOperatorImpl tairOperator;

  // result map
  public PlatformTairThread(Map<Long, PlatformData> map) {
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
        PlatformData data = map.get(key);
        PlatformData dataOld = syncMap.get(key);
        if (dataOld == null) {
          tairOperator.write(RaceConfig.prex_taobao + key, data.getTaobao());
          tairOperator.write(RaceConfig.prex_tmall + key, data.getTmall());
          syncMap.put(key, data);
        } else {
          if (data.getTaobao() != dataOld.getTaobao()) {
            tairOperator.write(RaceConfig.prex_taobao + key, data.getTaobao());
            syncMap.put(key, data);
          }
          if (data.getTmall() != dataOld.getTmall()) {
            tairOperator.write(RaceConfig.prex_tmall + key, data.getTmall());
            syncMap.put(key, data);
          }
        }
      }
    }
  }
}
