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
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.factory.SentryClientInvocationHandler;
import org.apache.sentry.service.thrift.factory.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolClientInvocationHandler<T extends SentryPolicyServiceBaseClient> implements SentryClientInvocationHandler<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PoolClientInvocationHandler.class);

  private final Configuration conf;
  private PooledObjectFactory<T> poolFactory;
  private GenericObjectPool<T> pool;
  private GenericObjectPoolConfig poolConfig;

  private static final String POOL_EXCEPTION_MESSAGE = "Pool exception occured ";

  public PoolClientInvocationHandler(Configuration conf, SentryServiceClientFactory<T> factory) throws Exception {
    this.conf = conf;
    readConfiguration();
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

  private void readConfiguration() {
    poolConfig = new GenericObjectPoolConfig();

    poolConfig.setMaxTotal(conf.getInt(ClientConfig.SENTRY_POOL_MAX_TOTAL, ClientConfig.SENTRY_POOL_MAX_TOTAL_DEFAULT));
    poolConfig.setMinIdle(conf.getInt(ClientConfig.SENTRY_POOL_MIN_IDLE, ClientConfig.SENTRY_POOL_MIN_IDLE_DEFAULT));
    poolConfig.setMaxIdle(conf.getInt(ClientConfig.SENTRY_POOL_MAX_IDLE, ClientConfig.SENTRY_POOL_MAX_IDLE_DEFAULT));

    poolConfig.setLifo(conf.getBoolean(ClientConfig.SENTRY_POOL_LIFO, ClientConfig.SENTRY_POOL_LIFO_DEFAULT));
    poolConfig.setMaxWaitMillis(conf.getLong(ClientConfig.SENTRY_POOL_MAX_WAIT_MILLIS,
        ClientConfig.SENTRY_POOL_MAX_WAIT_MILLIS_DEFAULT));
    poolConfig.setMinEvictableIdleTimeMillis(conf.getLong(ClientConfig.SENTRY_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS,
        ClientConfig.SENTRY_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS_DEFAULT));
    poolConfig.setSoftMinEvictableIdleTimeMillis(conf.getLong(ClientConfig.SENTRY_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS,
        ClientConfig.SENTRY_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS_DEFAULT));
    poolConfig.setNumTestsPerEvictionRun(conf.getInt(ClientConfig.SENTRY_POOL_NUM_TESTS_PER_EVICTION_RUN,
        ClientConfig.SENTRY_POOL_NUM_TESTS_PER_EVICTION_RUN_DEFAULT));
    poolConfig.setTestOnCreate(conf.getBoolean(ClientConfig.SENTRY_POOL_TEST_ON_CREATE,
        ClientConfig.SENTRY_POOL_TEST_ON_CREATE_DEFAULT));
    poolConfig.setTestOnBorrow(conf.getBoolean(ClientConfig.SENTRY_POOL_TEST_ON_BORROW,
        ClientConfig.SENTRY_POOL_TEST_ON_BORROW_DEFAULT));
    poolConfig.setTestOnReturn(conf.getBoolean(ClientConfig.SENTRY_POOL_TEST_ON_RETURN,
        ClientConfig.SENTRY_POOL_TEST_ON_RETURN_DEFAULT));
    poolConfig.setTestWhileIdle(conf.getBoolean(ClientConfig.SENTRY_POOL_TEST_WHILE_IDLE,
        ClientConfig.SENTRY_POOL_TEST_WHILE_IDLE_DEFAULT));
    poolConfig.setTimeBetweenEvictionRunsMillis(conf.getLong(ClientConfig.SENTRY_POOL_TIME_BETWEEN_EVICTION_RUNS_MILLIS,
        ClientConfig.SENTRY_POOL_TIME_BETWEEN_EVICTION_RUNS_MILLIS_DEFAULT));
    poolConfig.setBlockWhenExhausted(conf.getBoolean(ClientConfig.SENTRY_POOL_BLOCK_WHEN_EXHAUSTED,
        ClientConfig.SENTRY_POOL_BLOCK_WHEN_EXHAUSTED_DEFAULT));
  }
}
