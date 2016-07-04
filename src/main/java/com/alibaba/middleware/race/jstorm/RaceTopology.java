package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;

/**
 * Created by yfy on 7/2/16.
 * RaceTopology
 */
public class RaceTopology {
  public static void main(String[] args) {
    Config conf = new Config();
    //conf.setNumWorkers(4);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new MessageSpout(), 1);
    builder.setBolt("payRatio", new PayRatioBolt(), 1).shuffleGrouping("spout");
    //builder.setBolt("realTimePay", new RealTimePayBolt(), 1).shuffleGrouping("spout");

//    LocalCluster cluster = new LocalCluster();
//    cluster.submitTopology(RaceConfig.JstormTopologyName, conf, builder.createTopology());

    try {
      StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, conf, builder.createTopology());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
