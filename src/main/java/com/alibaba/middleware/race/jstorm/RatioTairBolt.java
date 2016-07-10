package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.Map;
import java.util.Set;

/**
 * Created by yfy on 7/10/16.
 * RatioTairBolt
 */
public class RatioTairBolt implements IRichBolt {

  private OutputCollector collector;

  //private Map<Long, RatioData> resultMap;

  private Set<Map.Entry<Long, RatioData>> set;

  private TairOperatorImpl tairOperator;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
//    tairOperator = TairOperatorImpl.getRaceTairOperator();
    tairOperator = TairOperatorImpl.getTestTairOperator();
  }

  @Override
  public void execute(Tuple tuple) {
    set = (Set) tuple.getValue(0);
    long id = tuple.getLong(1);
    writeTair();
    collector.ack(tuple);
  }

  private void writeTair() {
    for (Map.Entry<Long, RatioData> e : set) {
      tairOperator.write(RaceConfig.prex_ratio + e.getKey(), e.getValue().ratio());
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
