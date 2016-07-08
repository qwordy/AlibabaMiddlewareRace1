package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.Map;

/**
 * Created by yfy on 7/4/16.
 * PlatformBolt
 */
public class PlatformBolt implements IRichBolt {

  private OutputCollector collector;

  private PlatformThread platformThread;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;

    platformThread = new PlatformThread();
    new Thread(platformThread).start();
  }

  @Override
  public void execute(Tuple tuple) {
    MyMessage msg = (MyMessage) tuple.getValue(0);
    //RaceUtils.printMsg(msg, "[PlatformBolt]");
    deal(msg);
    collector.ack(tuple);
  }

  private void deal(MyMessage msg) {
    byte[] body = msg.getBody();
    if (body.length == 2 && body[0] == 0 && body[1] == 0)
      return;

    String topic = msg.getTopic();
    if (topic.equals(RaceConfig.MqPayTopic)) {
      PaymentMessage pm = RaceUtils.readKryoObject(PaymentMessage.class, body);
      platformThread.addPaymentMessage(pm);
    } else {
      OrderMessage om = RaceUtils.readKryoObject(OrderMessage.class, body);
      platformThread.addOrderMessage(om, topic);
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