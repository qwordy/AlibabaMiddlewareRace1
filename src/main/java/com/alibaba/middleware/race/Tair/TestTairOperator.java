package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceUtils;

import java.io.Serializable;

/**
 * Created by yfy on 7/7/16.
 * TestTairOperator
 */
public class TestTairOperator extends TairOperatorImpl {

  public TestTairOperator() {}

  @Override
  public boolean write(Serializable key, Serializable value) {
    RaceUtils.println("[Tair] " + key + ' ' + value);
    return true;
  }
}
