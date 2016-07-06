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
import java.util.TreeMap;

/**
 * Created by yfy on 7/4/16.
 * PayRatioBolt
 */
public class PayRatioBolt implements IRichBolt {

  private OutputCollector collector;

  // time, payRatioData
  private TreeMap<Long, PayRatioData> resultMap;

  //private WriteTairThread writeTairThread;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    resultMap = new TreeMap<>();

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

    PaymentMessage pm = RaceUtils.readKryoObject(PaymentMessage.class, body);
    long minuteTime = (pm.getCreateTime() / 1000 / 60) * 60;
    PayRatioData data;
    synchronized (resultMap) {
       data = resultMap.get(minuteTime);
    }

    if (data == null) {
      Map.Entry<Long, PayRatioData> entry;
      synchronized (resultMap) {
        entry = resultMap.lowerEntry(minuteTime);
      }
      if (entry == null)
        data = new PayRatioData();
      else
        data = new PayRatioData(entry.getValue());
    }

    if (pm.getPayPlatform() == 0) {  // pc
      data.addPc(pm.getPayAmount());
      synchronized (resultMap) {
        resultMap.put(minuteTime, data);
      }
      //writeTairThread.addPair(new Pair(RaceConfig.prex_ratio + minuteTime, data.ratio()));
      long time = minuteTime;
      while (true) {  // add later minute pay sum
        Map.Entry<Long, PayRatioData> entry;
        synchronized (resultMap) {
          entry = resultMap.higherEntry(time);
        }
        if (entry == null) {
          break;
        } else {
          entry.getValue().addPc(pm.getPayAmount());
          //writeTairThread.addPair(
          //    new Pair(RaceConfig.prex_ratio + time, entry.getValue().ratio()));
          time = entry.getKey();
        }
      }
    } else {  // wireless
      data.addWireless(pm.getPayAmount());
      synchronized (resultMap) {
        resultMap.put(minuteTime, data);
      }
      //writeTairThread.addPair(new Pair(RaceConfig.prex_ratio + minuteTime, data.ratio()));
      long time = minuteTime;
      while (true) {
        Map.Entry<Long, PayRatioData> entry;
        synchronized (resultMap) {
          entry = resultMap.higherEntry(time);
        }
        if (entry == null) {
          break;
        } else {
          entry.getValue().addWireless(pm.getPayAmount());
//          writeTairThread.addPair(
//              new Pair(RaceConfig.prex_ratio + time, entry.getValue().ratio()));
          time = entry.getKey();
        }
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
