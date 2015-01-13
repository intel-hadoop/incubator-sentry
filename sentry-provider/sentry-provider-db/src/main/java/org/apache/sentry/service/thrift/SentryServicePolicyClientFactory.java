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
package org.apache.sentry.service.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClientDefaultImpl;
import org.apache.sentry.service.thrift.factory.SentryServiceClientFactory;
import org.apache.sentry.service.thrift.factory.SentryServiceClientProxyFactory;

/**
 * SentryServicePolicyClientFactory is a public class for the component like HIVE creating sentry client.
 */
public class SentryServicePolicyClientFactory implements SentryServiceClientFactory<SentryPolicyServiceClient> {

  private SentryServiceClientFactory<SentryPolicyServiceClient> factory;
  private Configuration conf;

  public SentryServicePolicyClientFactory(Configuration conf) {
    this.conf = conf;
    initDefaultFactory();
  }

  private void initDefaultFactory() {
    factory = new SentryServiceClientProxyFactory<SentryPolicyServiceClient>(conf, new DefaultSentryServiceClientFactory(conf), SentryPolicyServiceClient.class);
  }

  @Override
  public SentryPolicyServiceClient getSentryPolicyClient() throws Exception {
    return factory.getSentryPolicyClient();
  }

  @Override
  public void close() {
    factory.close();
  }

  /**
   * By default, DefaultSentryServiceClientFactory create a SentryPolicyServiceClientDefaultImpl.
   */
  public class DefaultSentryServiceClientFactory implements SentryServiceClientFactory<SentryPolicyServiceClient> {

    private Configuration conf;

    public DefaultSentryServiceClientFactory(Configuration conf) {
      this.conf = conf;
    }

    public SentryPolicyServiceClient getSentryPolicyClient() throws Exception {
      return new SentryPolicyServiceClientDefaultImpl(conf);
    }

    @Override
    public void close() {
    }

  }
}
