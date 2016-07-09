package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.MyMessage;

/**
 * Created by yfy on 7/8/16
 * Test
 */
public class Test {
  public static void main(String[] args) {
    MyMessage msg = new MyMessage("aa", "bb", new byte[]{1,2});
    RaceUtils.readKryoObject(MyMessage.class, RaceUtils.writeKryoObject(msg));
  }
}
