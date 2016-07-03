package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by yfy on 7/3/16.
 * MessageSpout
 */
public class MessageSpout implements IRichSpout {

  private SpoutOutputCollector collector;

  //private BlockingQueue<> queue;

  @Override
  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    collector = spoutOutputCollector;

    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    try {
      consumer.subscribe(RaceConfig.MqPayTopic, "*");
      consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
      consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
    } catch (Exception e) {
      e.printStackTrace();
    }

    consumer.registerMessageListener(new MessageListenerConcurrently() {
      @Override
      public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        for (MessageExt msg : list) {

        }
        return null;
      }
    });
  }

  @Override
  public void nextTuple() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(""));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public void activate() {

  }

  @Override
  public void deactivate() {

  }

  @Override
  public void ack(Object o) {

  }

  @Override
  public void fail(Object o) {

  }
}
