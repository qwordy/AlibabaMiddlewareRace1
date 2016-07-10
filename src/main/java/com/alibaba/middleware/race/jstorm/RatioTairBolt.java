package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by yfy on 7/10/16.
 * RatioTairBolt
 */
public class RatioTairBolt implements IRichBolt {

  private OutputCollector collector;

  // id, time, data
  private Map<Long, Map<Long, RatioData>> resultMapMap;

  // last sync data
  private Map<Long, Double> syncMap;

  private TairOperatorImpl tairOperator;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    resultMapMap = new HashMap<>();
    syncMap = new HashMap<>();

    tairOperator = TairOperatorImpl.getRaceTairOperator();
//    tairOperator = TairOperatorImpl.getTestTairOperator();
  }

  private void writeTair() {
    //RaceUtils.println("resultMapMap size: " + resultMapMap.size());

    // sum up a result map
    Map<Long, RatioData> resultMap = new HashMap<>();
    for (long id : resultMapMap.keySet()) {
      Map<Long, RatioData> map = resultMapMap.get(id);
      //RaceUtils.println("smallMap id: " + id);
      for (long time : map.keySet()) {
        RatioData data = map.get(time);
        //RaceUtils.println(time + " " + data.ratio());
        RatioData dataSum = resultMap.get(time);
        if (dataSum == null)
          resultMap.put(time, data);
        else
          dataSum.addRatioData(data);
      }
    }

    //RaceUtils.println("resultMap:");
    for (long key : resultMap.keySet()) {
      double ratio = resultMap.get(key).ratio();
      //RaceUtils.println(key + " " + ratio);
      Double ratioOld = syncMap.get(key);
      if (ratioOld == null || ratio != ratioOld) {
        tairOperator.write(RaceConfig.prex_ratio + key, ratio);
        syncMap.put(key, ratio);
      }
    }
  }

  @Override
  public void execute(Tuple tuple) {
    if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
        tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
      writeTair();
    } else {
      long id = tuple.getLong(1);
      Map<Long, RatioData> map = (Map<Long, RatioData>) tuple.getValue(0);
      resultMapMap.put(id, map);
      collector.ack(tuple);
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
    Map<String, Object> conf = new Config();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
    return conf;
  }
}
