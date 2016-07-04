package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/3/16.
 * MessageSpout
 */
public class MessageSpout implements IRichSpout {

  private SpoutOutputCollector collector;

  private BlockingQueue<MessageExt> queue;

  @Override
  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    collector = spoutOutputCollector;

    queue = new LinkedBlockingQueue<>(500);

    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    try {
      consumer.subscribe(RaceConfig.MqPayTopic, "*");
      consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
      consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");

      consumer.registerMessageListener(new MessageListenerConcurrently() {
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
          try {
            for (MessageExt msg : list) {
              queue.put(msg);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          } catch (Exception e) {
            e.printStackTrace();
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
          }
        }
      });

      consumer.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void nextTuple() {
    try {
      MessageExt msg = queue.take();
      //RaceUtils.printMsg(msg, "[MessageSpout]");
      collector.emit(new Values(msg));
    } catch (Exception e) {
      e.printStackTrace();
    }
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
