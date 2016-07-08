package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/5/16.
 * WriteTairThread
 */
public class WriteTairThread implements Runnable {

  private LinkedBlockingQueue<Pair> queue;

  private TairOperatorImpl tairOperator;

  public WriteTairThread() {
    queue = new LinkedBlockingQueue<>(100000);

//    tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
//        RaceConfig.TairSalveConfigServer,
//        RaceConfig.TairGroup, RaceConfig.TairNamespace);
    tairOperator = new TairOperatorImpl();
  }

  public void addPair(Pair pair) {
    try {
      //RaceUtils.println("[RatioBolt] " + pair.key + ' ' + pair.value);
      queue.put(pair);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    try {
      Pair p, q;
      while (true) {
        q = queue.take();
        while (true) {
          p = queue.poll();
          if (p == null) {
            tairOperator.write(q.key, q.value);
            break;
          }
          if (!p.key.equals(q.key)) {
            tairOperator.write(q.key, q.value);
            tairOperator.write(p.key, p.value);
            break;
          }
          q = p;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
