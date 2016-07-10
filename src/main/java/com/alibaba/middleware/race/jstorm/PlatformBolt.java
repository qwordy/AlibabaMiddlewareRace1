package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yfy on 7/4/16.
 * PlatformBolt
 */
public class PlatformBolt implements IRichBolt {

  private OutputCollector collector;

  private PlatformThread platformThread;

  // orderId, msg
  private ConcurrentHashMap<Long, MyOrderMessage> orderMap;

  private ConcurrentHashMap<Long, PlatformData> resultMap;

  private byte[] lock;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    orderMap = new ConcurrentHashMap<>();
    resultMap = new ConcurrentHashMap<>();
    lock = new byte[0];

    platformThread = new PlatformThread(orderMap, resultMap, lock);
    new Thread(platformThread).start();

    new Thread(new PlatformTairThread(resultMap)).start();
  }

  @Override
  public void execute(Tuple tuple) {
    MyMessage msg = (MyMessage) tuple.getValue(0);
    //RaceUtils.printMsg(msg, "[PlatformBolt]");
    deal(msg);
    collector.ack(tuple);
  }

  private void deal(MyMessage msg) {
    byte[] body = msg.getBody();
    if (body.length == 2 && body[0] == 0 && body[1] == 0)
      return;

    String topic = msg.getTopic();
    if (topic.equals(RaceConfig.MqPayTopic)) {
      PaymentMessage pm = RaceUtils.readKryoObject(PaymentMessage.class, body);
      dealPay(pm);
    } else {
      OrderMessage om = RaceUtils.readKryoObject(OrderMessage.class, body);
      short platform = topic.equals(RaceConfig.MqTaobaoTradeTopic) ?
          MyOrderMessage.TAOBAO : MyOrderMessage.TMALL;
      orderMap.put(om.getOrderId(), new MyOrderMessage(platform, om.getTotalPrice()));
    }
  }

  private void dealPay(PaymentMessage pm) {
    long orderId = pm.getOrderId();
    MyOrderMessage om = orderMap.get(orderId);

    if (om == null) {
      platformThread.addPay(pm);
    } else {
      double payAmount = pm.getPayAmount();
      long minuteTime = (pm.getCreateTime() / 1000 / 60) * 60;

      PlatformData data;
      synchronized (lock) {
        data = resultMap.get(minuteTime);
        if (data == null) {
          data = new PlatformData();
          resultMap.put(minuteTime, data);
        }
      }

      if (om.taobao())
        data.addTaobao(payAmount);
      else
        data.addTmall(payAmount);

      if (om.minusPrice(payAmount))
        orderMap.remove(orderId);
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
