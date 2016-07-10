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

  private Map<Long, PlatformData> resultMap;

  private Map<Long, PlatformData> syncMap;

  private TairOperatorImpl tairOperator;

  public PlatformTairThread(Map<Long, PlatformData> resultMap) {
    this.resultMap = resultMap;
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

      for (long key : resultMap.keySet()) {
        PlatformData data = resultMap.get(key);
        PlatformData dataOld = syncMap.get(key);
        if (dataOld == null) {
          tairOperator.write(RaceConfig.prex_taobao + key, data.getTaobao());
          tairOperator.write(RaceConfig.prex_tmall + key, data.getTmall());
          syncMap.put(key, new PlatformData(data));
        } else {
          if (data.getTaobao() != dataOld.getTaobao()) {
            tairOperator.write(RaceConfig.prex_taobao + key, data.getTaobao());
            dataOld.setTaobao(data.getTaobao());
          }
          if (data.getTmall() != dataOld.getTmall()) {
            tairOperator.write(RaceConfig.prex_tmall + key, data.getTmall());
            dataOld.setTmall((data.getTmall()));
          }
        }
      }
    }
  }
}
