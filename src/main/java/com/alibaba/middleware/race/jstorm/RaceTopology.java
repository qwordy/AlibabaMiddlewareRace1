package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;

/**
 * Created by yfy on 7/2/16.
 * RaceTopology.
 * Change pom.xml, topology(submit), log, tair(all!), consumerAddr before submit
 */
public class RaceTopology {
  public static void main(String[] args) {
    Config conf = new Config();
    conf.setNumWorkers(3);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new MessageSpout(), 1);
    builder.setBolt("ratio", new RatioBolt(), 1).shuffleGrouping("spout");
    //builder.setBolt("platform", new PlatformBolt(), 1).shuffleGrouping("spout");

//    RaceUtils.initLog();
//    LocalCluster cluster = new LocalCluster();
//    cluster.submitTopology(RaceConfig.JstormTopologyName, conf, builder.createTopology());

    try {
      StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, conf, builder.createTopology());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
