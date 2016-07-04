package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/4/16.
 * PayRatioBolt
 */
public class PayRatioBolt implements IRichBolt {

  private OutputCollector collector;

  // time, paySum
  private Map<Long, PayRatioData> map;

  private int count;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    this.map = new HashMap<>();
  }

  @Override
  public void execute(Tuple tuple) {
    MessageExt msg = (MessageExt) tuple.getValue(0);
    RaceUtils.printMsg(msg, "[PayRatioBolt]");
    deal(msg);
    collector.ack(tuple);
  }

  private void deal(MessageExt msg) {
    if (!msg.getTopic().equals(RaceConfig.MqPayTopic))
      return;

    count++;
    byte[] body = msg.getBody();
    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
      RaceUtils.println(count);
      RaceUtils.printPayRatio(map);
    } else {
      PaymentMessage pm = RaceUtils.readKryoObject(PaymentMessage.class, body);
      long minuteTime = (pm.getCreateTime() / 1000 / 60) * 60;
      PayRatioData data = map.get(minuteTime);
      if (data == null) {
        if (pm.getPayPlatform() == 0)  // pc
          map.put(minuteTime, new PayRatioData(0, pm.getPayAmount()));
        else
          map.put(minuteTime, new PayRatioData(pm.getPayAmount(), 0));
      } else {
        if (pm.getPayPlatform() == 0)  // pc
          data.addPc(pm.getPayAmount());
        else
          data.addWireless(pm.getPayAmount());
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
