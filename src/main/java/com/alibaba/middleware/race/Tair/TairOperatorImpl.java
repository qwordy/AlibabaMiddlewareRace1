package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

  private DefaultTairManager manager;

  private int namespace;

  public TairOperatorImpl(String masterConfigServer,
                          String slaveConfigServer,
                          String groupName,
                          int namespace) {
    List<String> configServerList = new ArrayList<>();
    configServerList.add(masterConfigServer);
    configServerList.add(slaveConfigServer);

    manager = new DefaultTairManager();
    manager.setConfigServerList(configServerList);
    manager.setGroupName(groupName);
    manager.init();

    this.namespace = namespace;
  }

  public TairOperatorImpl() {}

  public boolean write(Serializable key, Serializable value) {
    ResultCode code;
    for (int i = 0; i < 20; i++) {
      code = manager.put(namespace, key, value);
      if (code.isSuccess()) return true;
    }
    return false;
  }

  public void write2(Serializable key, Serializable value) {
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    //RaceUtils.println("[Tair] " + key + ' ' + value);
  }

  public Object get(Serializable key) {
    Result<DataEntry> result = manager.get(namespace, key);
    if (result.isSuccess()) {
      DataEntry entry = result.getValue();
      if (entry != null)
        return entry.getValue();
      else
        return null;
    } else {
      return null;
    }
  }

  public boolean remove(Serializable key) {
    ResultCode code = manager.delete(namespace, key);
    return code.isSuccess();
  }

  public void close() {
    manager.close();
  }

  //天猫的分钟交易额写入tair
  public static void main(String[] args) throws Exception {
    TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
        RaceConfig.TairSalveConfigServer,
        RaceConfig.TairGroup, RaceConfig.TairNamespace);
    //假设这是付款时间
    Long millisTime = System.currentTimeMillis();
    //由于整分时间戳是10位数，所以需要转换成整分时间戳
    Long minuteTime = (millisTime / 1000 / 60) * 60;
    //假设这一分钟的交易额是100;
    Double money = 100.0;
    //写入tair
    tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
  }
}
