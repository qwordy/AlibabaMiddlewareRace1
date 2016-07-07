package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yfy on 7/4/16.
 * PayRatioBolt
 */
public class PayRatioBolt implements IRichBolt {

  private OutputCollector collector;

  // time, payRatioData
  private ConcurrentHashMap<Long, PayRatioData> resultMap;

  //private WriteTairThread writeTairThread;

  private int payCount = 0;

  private long minTime, maxTime;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    resultMap = new ConcurrentHashMap<>();

    minTime = 9999999999L;
    maxTime = 0;

//    writeTairThread = new WriteTairThread();
//    new Thread(writeTairThread).start();

    new Thread(new PayRatioWriteTairThread(resultMap)).start();
  }

  @Override
  public void execute(Tuple tuple) {
    MyMessage msg = (MyMessage) tuple.getValue(0);
    //RaceUtils.printMsg(msg, "[PayRatioBolt]");
    deal(msg);
    collector.ack(tuple);
  }

  private void deal(MyMessage msg) {
    if (!msg.getTopic().equals(RaceConfig.MqPayTopic))
      return;

    byte[] body = msg.getBody();
    if (body.length == 2 && body[0] == 0 && body[1] == 0)
      return;

    //System.out.println("[PayRatio] " + (++payCount));

    PaymentMessage pm = RaceUtils.readKryoObject(PaymentMessage.class, body);
    long minuteTime = (pm.getCreateTime() / 1000 / 60) * 60;
    PayRatioData data = resultMap.get(minuteTime);

    if (data == null) {
      if (minuteTime > minTime) {
        PayRatioData d = null;
        long t;

        for (t = minuteTime - 60; t >= minTime; t -= 60) {
          d = resultMap.get(t);
          if (d != null) {
            data = new PayRatioData(d);
            break;
          }
        }
        while ((t += 60) < minuteTime)
          resultMap.put(t, new PayRatioData(d));
      } else {
        data = new PayRatioData();
      }
    }

    if (pm.getPayPlatform() == 0)  // pc
      data.addPc(pm.getPayAmount());
    else
      data.addWireless(pm.getPayAmount());
    resultMap.put(minuteTime, data);

    // update later time
    if (minuteTime < maxTime) {
      long t = minuteTime + 60;
      while (t <= maxTime) {
        PayRatioData d = resultMap.get(t);
        if (d != null) {
          if (pm.getPayPlatform() == 0)  // pc
            d.addPc(pm.getPayAmount());
          else
            d.addWireless(pm.getPayAmount());
        }
        t += 60;
      }
    }
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
