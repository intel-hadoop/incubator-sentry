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

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceBaseClient;
import org.apache.sentry.service.thrift.factory.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SentryServiceClientPoolFactory is for connection pool to manage the object. Implement the related
 * method to create object, destroy object and wrap object.
 */

public class SentryServiceClientPoolFactory<T extends SentryPolicyServiceBaseClient> extends BasePooledObjectFactory<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceClientPoolFactory.class);

  private SentryServiceClientFactory<T> factory;

  public SentryServiceClientPoolFactory(SentryServiceClientFactory<T> factory) {
    LOGGER.debug("Creating Sentry Service Client Factory..." + factory.getClass());
    this.factory = factory;
  }

  @Override
  public T create() throws Exception {
    LOGGER.debug("Creating Sentry Service Client...");
    return factory.getSentryPolicyClient();
  }

  @Override
  public PooledObject<T> wrap(T client) {
    return new DefaultPooledObject<T>(client);
  }

  @Override
  public void destroyObject(PooledObject<T> pooledObject) {
    SentryPolicyServiceBaseClient client = pooledObject.getObject();
    LOGGER.debug("Destroying Sentry Service Client: " + client);
    if (client != null) {
      // The close() of TSocket or TSaslClientTransport is called actually, and there has no
      // exception even there has some problems, eg, the client is closed already.
      // The close here is just try to close the socket and the client will be destroyed soon.
      client.close();
    }
  }

}