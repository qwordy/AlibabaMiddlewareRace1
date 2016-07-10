package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.Map;

/**
 * Created by yfy on 7/10/16.
 * RatioTairBolt
 */
public class RatioTairBolt implements IRichBolt {

  private OutputCollector collector;

  private Map<Long, RatioData> resultMap;

  private TairOperatorImpl tairOperator;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
//    tairOperator = TairOperatorImpl.getRaceTairOperator();
    tairOperator = TairOperatorImpl.getTestTairOperator();
  }

  @Override
  public void execute(Tuple tuple) {
    resultMap = (Map<Long, RatioData>) tuple.getValue(0);
    writeTair();
    collector.ack(tuple);
  }

  private void writeTair() {
    for (long key : resultMap.keySet()) {
      double ratio = resultMap.get(key).ratio();
      tairOperator.write(RaceConfig.prex_ratio + key, ratio);
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
