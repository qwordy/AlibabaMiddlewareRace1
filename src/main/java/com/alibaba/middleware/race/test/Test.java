package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.MyMessage22;

/**
 * Created by yfy on 7/8/16
 * Test
 */
public class Test {
  public static void main(String[] args) {
    MyMessage22 msg = new MyMessage22("aa", "bb", new byte[]{1,2});
    RaceUtils.readKryoObject(MyMessage22.class, RaceUtils.writeKryoObject(msg));
  }
}
