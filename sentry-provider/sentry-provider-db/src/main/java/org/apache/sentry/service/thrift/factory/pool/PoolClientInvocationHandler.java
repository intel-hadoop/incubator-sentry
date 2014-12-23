/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.service.thrift.factory.pool;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceBaseClient;
import org.apache.sentry.service.thrift.factory.SentryClientInvocationHandler;
import org.apache.sentry.service.thrift.factory.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolClientInvocationHandler<T extends SentryPolicyServiceBaseClient> implements SentryClientInvocationHandler<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PoolClientInvocationHandler.class);

  private final Configuration conf;
  private Integer maxActive = 32;
  private Integer idleTime = 1000;
  private PooledObjectFactory<T> poolFactory;
  private GenericObjectPool<T> pool;

  private static final String POOL_EXCEPTION_MESSAGE = "Pool exception occured ";

  public PoolClientInvocationHandler(Configuration conf, SentryServiceClientFactory<T> factory) throws Exception {
    this.conf = conf;
    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    // TODO read from conf
    poolConfig.setMaxTotal(this.conf.getInt(POOL_EXCEPTION_MESSAGE,maxActive));
    poolConfig.setMaxTotal(maxActive);
    poolConfig.setMinIdle(0);
    poolConfig.setMinEvictableIdleTimeMillis(idleTime);
    poolConfig.setTimeBetweenEvictionRunsMillis(idleTime/2L);
    poolFactory = new SentryServiceClientPoolFactory<T>(factory);
    pool = new GenericObjectPool<T>(poolFactory, poolConfig);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
    Object result = null;
    if (method.getName() == "close") {
      return result;
    }
    T client;
    try {
      client = pool.borrowObject();
    } catch (Exception e) {
      LOGGER.debug(POOL_EXCEPTION_MESSAGE, e);
      throw e;
    }
    try {
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      result = method.invoke(client, args);
    } catch (IllegalAccessException e) {
      throw new SentryUserException(e.getMessage(), e.getCause());
    } catch (InvocationTargetException e) {
      if (e.getTargetException() instanceof SentryUserException) {
        throw (SentryUserException)e.getTargetException();
      }
    } catch (Exception e) {
      throw e;
    } finally{
      try {
        pool.returnObject(client);
      } catch (Exception e) {
        LOGGER.debug(POOL_EXCEPTION_MESSAGE, e);
        throw e;
      }
    }
    return result;
  }

  @Override
  public void close() {
    try {
      pool.close();
    } catch (Exception e) {
      LOGGER.debug(POOL_EXCEPTION_MESSAGE, e);
    }
  }

}
