package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yfy on 7/6/16.
 * RealTimePayWriteTairThread.
 * Write tair periodly.
 */
public class RealTimePayWriteTairThread implements Runnable {

  private ConcurrentHashMap<Long, RealTimePayData> map;

  private TairOperatorImpl tairOperator;

  public RealTimePayWriteTairThread(ConcurrentHashMap<Long, RealTimePayData> map) {
    this.map = map;

//    tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
//        RaceConfig.TairSalveConfigServer,
//        RaceConfig.TairGroup, RaceConfig.TairNamespace);
    tairOperator = new TairOperatorImpl();
  }

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(10000);
        for (long key : map.keySet()) {
          RealTimePayData data = map.get(key);
          tairOperator.write(RaceConfig.prex_taobao + key, data.taobao);
          tairOperator.write(RaceConfig.prex_tmall + key, data.tmall);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
