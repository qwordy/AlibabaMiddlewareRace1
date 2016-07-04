package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.Map;

/**
 * Created by yfy on 7/4/16.
 * RealTimePayBolt
 */
public class RealTimePayBolt implements IRichBolt {

  private OutputCollector collector;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    MessageExt msg = (MessageExt) tuple.getValue(0);
    //RaceUtils.printMsg(msg, "[RealTimePayBolt]");
    collector.ack(tuple);
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
