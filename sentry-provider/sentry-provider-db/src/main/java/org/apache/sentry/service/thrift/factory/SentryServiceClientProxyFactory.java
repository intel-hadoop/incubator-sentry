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

package org.apache.sentry.service.thrift.factory;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceBaseClient;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.factory.ha.SentryHAClientFactory;
import org.apache.sentry.service.thrift.factory.pool.SentryPoolingClientFactory;

import com.google.common.base.Preconditions;

/**
 * SentryServiceClientProxyFactory is the factory composes the factories with different functions.
 */
public class SentryServiceClientProxyFactory<T extends SentryPolicyServiceBaseClient> implements SentryServiceClientFactory<T> {

  SentryServiceClientFactory<T> clientFactory;
  Class<T> typeParameterClass;

  public SentryServiceClientProxyFactory(Configuration conf, SentryServiceClientFactory<T> defaultFactory, Class<T> typeParameterClass) {
    Preconditions.checkNotNull(defaultFactory);
    this.clientFactory = defaultFactory;
    this.typeParameterClass = typeParameterClass;
    boolean haEnabled = conf.getBoolean(ClientConfig.SERVER_HA_ENABLED, ClientConfig.SERVER_HA_ENABLED_DEFAULT);
    if (haEnabled) {
      clientFactory = new SentryHAClientFactory<T>(conf, clientFactory, typeParameterClass);
    }
    boolean pooled = conf.getBoolean(ClientConfig.SENTRY_POOL_ENABLED,
        ClientConfig.SENTRY_POOL_ENABLED_DEFAULT);
    if (pooled) {
      clientFactory = new SentryPoolingClientFactory<T>(conf, clientFactory, typeParameterClass);
    }
  }

  public T getSentryPolicyClient() throws Exception {
    return (T) clientFactory.getSentryPolicyClient();
  }

  @Override
  public void close() {
    clientFactory.close();
  }

}
