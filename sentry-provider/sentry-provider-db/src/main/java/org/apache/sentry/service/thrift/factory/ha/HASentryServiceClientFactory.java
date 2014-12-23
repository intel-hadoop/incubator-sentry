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

package org.apache.sentry.service.thrift.factory.ha;

import java.lang.reflect.Proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceBaseClient;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.factory.SentryClientInvocationHandler;
import org.apache.sentry.service.thrift.factory.SentryServiceClientFactory;

public class HASentryServiceClientFactory<T extends SentryPolicyServiceBaseClient> implements SentryServiceClientFactory<T> {

  private Configuration conf;
  private SentryServiceClientFactory<T> factory;
  private SentryClientInvocationHandler<T> handler;
  final Class<T> typeParameterClass;

  @SuppressWarnings("unused")
  private HASentryServiceClientFactory() {
    typeParameterClass = null;
  }

  public HASentryServiceClientFactory(Configuration conf, SentryServiceClientFactory<T> factory, Class<T> typeParameterClass) {
    this.conf = conf;
    this.factory = factory;
    this.typeParameterClass = typeParameterClass;
  }

  @SuppressWarnings("unchecked")
  public T getSentryPolicyClient() throws Exception {
    ensureInvocationHandler();
    boolean haEnabled = conf.getBoolean(ClientConfig.SENTRY_HA_ENABLED, ClientConfig.SENTRY_HA_ENABLED_DEFAULT);
    if (haEnabled) {
      if (typeParameterClass.isInterface()) {
        return (T) Proxy
            .newProxyInstance(typeParameterClass.getClassLoader(),
                new Class[]{typeParameterClass},
                handler);
      } else {
        return (T) Proxy
            .newProxyInstance(typeParameterClass.getClassLoader(),
                typeParameterClass.getInterfaces(),
                handler);
      }
    } else {
      return factory.getSentryPolicyClient();
    }
  }

  private void ensureInvocationHandler() throws Exception {
    if (handler == null) {
      synchronized(this) {
        if (handler == null) {
          handler = new HAClientInvocationHandler<T>(conf, factory);
        }
      }
    }
  }

  @Override
  public void close() {
    handler.close();
  }
}
