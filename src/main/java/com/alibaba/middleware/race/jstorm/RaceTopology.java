package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by yfy on 7/2/16.
 * RaceTopology
 */
public class RaceTopology {
  public static void main(String[] args) {
    Config conf = new Config();
    conf.setNumWorkers(4);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new MessageSpout(), 1);
  }
}
