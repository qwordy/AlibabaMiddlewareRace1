package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yfy on 7/3/16.
 * MessageSpout
 */
public class MessageSpout implements IRichSpout {

  private SpoutOutputCollector collector;

  private BlockingQueue<MyMessage> queue;

//  private int count;
//
//  private AtomicInteger count2;

  @Override
  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    collector = spoutOutputCollector;

    int MAX_SIZE = 1000000;
    queue = new LinkedBlockingQueue<>(MAX_SIZE);

//    count = 0;
//    count2 = new AtomicInteger();

    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    consumer.setConsumeMessageBatchMaxSize(32);
    consumer.setPullBatchSize(32);

    //consumer.setNamesrvAddr("127.0.0.1:9876");

    try {
      consumer.subscribe(RaceConfig.MqPayTopic, "*");
      consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
      consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");

      consumer.registerMessageListener(new MessageListenerConcurrently() {
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
          //System.out.println(Thread.currentThread().getId());
          try {
            for (MessageExt msg : list) {
              //RaceUtils.println("[msgListSize] " + list.size());
              //RaceUtils.println("[MessageId] " + msg.getMsgId());
              //System.out.println("[spoutConsume] " + (count2.incrementAndGet()));
              queue.put(new MyMessage(msg.getMsgId(), msg.getTopic(), msg.getBody()));
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
    //System.out.println(Thread.currentThread().getId());
    MyMessage msg = queue.poll();
    while (msg != null) {
      //System.out.println("[spoutEmit] " + (++count));
      collector.emit(new Values(msg));
      msg = queue.poll();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("msg"));
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
