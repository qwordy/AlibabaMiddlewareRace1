package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yfy on 7/4/16.
 * RatioBolt
 */
public class RatioBolt implements IRichBolt {

  private OutputCollector collector;

  // time, ratioData
  private ConcurrentHashMap<Long, RatioData> resultMap;

  //private WriteTairThread writeTairThread;

  //private int payCount;

  private long minTime, maxTime;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    resultMap = new ConcurrentHashMap<>();

    minTime = 9999999999L;
    maxTime = 0;

    //payCount = 0;

//    writeTairThread = new WriteTairThread();
//    new Thread(writeTairThread).start();

    new Thread(new RatioTairThread(resultMap)).start();
  }

  private void writeTair() {
    //RaceUtils.println("[Ratio] writeTair");
  }

  @Override
  public void execute(Tuple tuple) {
//    if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
//        tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
//      writeTair();
//      return;
//    }

    System.out.println((String)tuple.getValue(0));
    //MyMessage22 msg = (MyMessage22) tuple.getValue(0);
    //RaceUtils.printMsg(msg, "[RatioBolt]");
    //deal(msg);
    collector.ack(tuple);
  }

  private void deal(MyMessage22 msg) {
    if (!msg.getTopic().equals(RaceConfig.MqPayTopic))
      return;

    byte[] body = msg.getBody();
    if (body.length == 2 && body[0] == 0 && body[1] == 0)
      return;

    //System.out.println("[PayRatio] " + (++payCount));

    PaymentMessage pm = RaceUtils.readKryoObject(PaymentMessage.class, body);
    long minuteTime = (pm.getCreateTime() / 1000 / 60) * 60;
    RatioData data = resultMap.get(minuteTime);

    if (data == null) {
      if (minuteTime > minTime) {
        RatioData d = null;
        long t;

        for (t = minuteTime - 60; t >= minTime; t -= 60) {
          d = resultMap.get(t);
          if (d != null) {
            data = new RatioData(d);
            break;
          }
        }
        while ((t += 60) < minuteTime)
          resultMap.put(t, new RatioData(d));
      } else {
        data = new RatioData();
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
        RatioData d = resultMap.get(t);
        if (d != null) {
          if (pm.getPayPlatform() == 0)  // pc
            d.addPc(pm.getPayAmount());
          else
            d.addWireless(pm.getPayAmount());
        }
        t += 60;
      }
    }

    // update min, max
    if (minuteTime > maxTime)
      maxTime = minuteTime;
    if (minuteTime < minTime)
      minTime = minuteTime;
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
//    Map<String, Object> conf = new Config();
//    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
//    return conf;

    return null;
  }
}
