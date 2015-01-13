/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServicePolicyClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

public class SimpleDBProviderBackend implements ProviderBackend {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleDBProviderBackend.class);
  private SentryServicePolicyClientFactory clientFactory;
  private SentryPolicyServiceClient policyServiceClient;

  private volatile boolean initialized;

  public SimpleDBProviderBackend(Configuration conf, String resourcePath) throws Exception {
    // DB Provider doesn't use policy file path
    this(conf);
  }

  public SimpleDBProviderBackend(Configuration conf) throws Exception {
    this.initialized = false;
    this.clientFactory = new SentryServicePolicyClientFactory(conf);
  }

  @VisibleForTesting
  public SimpleDBProviderBackend(SentryPolicyServiceClient policyServiceClient) throws IOException {
    this.initialized = false;
    this.policyServiceClient = policyServiceClient;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(ProviderBackendContext context) {
    if (initialized) {
      throw new IllegalStateException("Backend has already been initialized, cannot be initialized twice");
    }
    this.initialized = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    return getPrivileges(1, groups, roleSet, authorizableHierarchy);
  }

  private ImmutableSet<String> getPrivileges(int retryCount, Set<String> groups, ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    if (!initialized) {
      throw new IllegalStateException("Backend has not been properly initialized");
    }
    try {
      return ImmutableSet.copyOf(getSentryClient().listPrivilegesForProvider(groups, roleSet, authorizableHierarchy));
    } catch (Exception e) {
      policyServiceClient = null;
      if (retryCount > 0) {
        return getPrivileges(retryCount - 1, groups, roleSet, authorizableHierarchy);
      } else {
        String msg = "Unable to obtain privileges from server: " + e.getMessage();
        LOGGER.error(msg, e);
        try {
          policyServiceClient.close();
        } catch (Exception ex2) {
          // Ignore
        }
      }
    }
    return ImmutableSet.of();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSet<String> getRoles(Set<String> groups, ActiveRoleSet roleSet) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void close() {
    if (policyServiceClient != null) {
      policyServiceClient.close();
    }
    if (clientFactory != null) {
      clientFactory.close();
    }
  }

  private SentryPolicyServiceClient getSentryClient() {
    if (policyServiceClient == null) {
      try {
        policyServiceClient = clientFactory.getSentryPolicyClient();
      } catch (Exception e) {
        LOGGER.error("Error connecting to Sentry ['{}'] !!",
            e.getMessage());
        policyServiceClient = null;
        return null;
      }
    }
    return policyServiceClient;
  }
  /**
   * SimpleDBProviderBackend does not implement validatePolicy()
   */
  @Override
  public void validatePolicy(boolean strictValidation) throws SentryConfigurationException {
    if (!initialized) {
      throw new IllegalStateException("Backend has not been properly initialized");
    }
    // db provider does not implement validation
  }
}
