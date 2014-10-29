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
package org.apache.sentry.binding.hive.v2;

import java.lang.reflect.Constructor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.impl.DefaultSentryAccessController;
import org.apache.sentry.binding.hive.v2.impl.DefaultSentryAuthorizationValidator;
import org.apache.sentry.binding.hive.v2.impl.SentryAuthorizerImpl;

public class SentryHiveAuthorizerFactory implements HiveAuthorizerFactory {
  public static String HIVE_SENTRY_ACCESS_CONTROLLER =
      "hive.security.sentry.access.controller";
  public static String HIVE_SENTRY_AUTHORIZATION_CONTROLLER =
      "hive.security.sentry.authorization.controller";

  @Override
  public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider authenticator) throws HiveAuthzPluginException {
    SentryAccessController accessController = getAccessController(conf, authenticator);
    SentryAuthorizationValidator authzValidator = getAuthzController(conf, authenticator);

    return new SentryAuthorizerImpl(accessController, authzValidator);
  }

  /**
   * just for testing
   */
  public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthzConf authzConf, HiveAuthenticationProvider authenticator)
          throws HiveAuthzPluginException {
    SentryAccessController accessController = getAccessController(conf, authenticator);
    SentryAuthorizationValidator authzValidator = getAuthzController(conf, authzConf, authenticator);

    return new SentryAuthorizerImpl(accessController, authzValidator);
  }

  /**
   * Get instance of SentryAccessController from configuration
   * Default return DefaultSentryAccessController
   *
   * @param conf
   * @param authenticator
   * @throws HiveAuthzPluginException
   */
  public static SentryAccessController getAccessController(HiveConf conf,
      HiveAuthenticationProvider authenticator) throws HiveAuthzPluginException {
    String name = HIVE_SENTRY_ACCESS_CONTROLLER;
    Class<? extends SentryAccessController> clazz = conf.getClass(name,
        DefaultSentryAccessController.class, SentryAccessController.class);

    if(clazz == null){
      //should not happen as default value is set
      throw new HiveAuthzPluginException("Configuration value " + name
          + " is not set to valid SentryAccessController subclass" );
    }

    SentryAccessController accessController = null;
    try {
      Constructor<? extends SentryAccessController> constructor =
          clazz.getConstructor(HiveConf.class, HiveAuthenticationProvider.class);
      accessController = (SentryAccessController) constructor.newInstance(conf, authenticator);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }

    return accessController;
  }

  /**
   * Get instance of SentryAuthorizationValidator from configuration
   * Default return DefaultSentryAuthorizationValidator
   *
   * @param conf
   * @param authenticator
   * @throws HiveAuthzPluginException
   */
  public static SentryAuthorizationValidator getAuthzController(HiveConf conf,
      HiveAuthenticationProvider authenticator) throws HiveAuthzPluginException {
    String name = HIVE_SENTRY_AUTHORIZATION_CONTROLLER;
    Class<? extends SentryAuthorizationValidator> clazz = conf.getClass(name,
        DefaultSentryAuthorizationValidator.class, SentryAuthorizationValidator.class);

    if (clazz == null) {
      // should not happen as default value is set
      throw new HiveAuthzPluginException("Configuration value " + name
          + " is not set to valid SentryAuthorizationValidator subclass");
    }

    SentryAuthorizationValidator authzController = null;
    try {
      Constructor<? extends SentryAuthorizationValidator> constructor =
          clazz.getConstructor(HiveConf.class, HiveAuthenticationProvider.class);
      authzController = (SentryAuthorizationValidator) constructor.newInstance(conf, authenticator);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }

    return authzController;
  }

  /**
   * Get instance of SentryAuthorizationValidator from configuration
   * Default return DefaultSentryAuthorizationValidator
   *
   * @param conf
   * @param authenticator
   * @param authzConf
   * @throws HiveAuthzPluginException
   */
  public static SentryAuthorizationValidator getAuthzController(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws HiveAuthzPluginException {
    String name = HIVE_SENTRY_AUTHORIZATION_CONTROLLER;
    Class<? extends SentryAuthorizationValidator> clazz = conf.getClass(name,
        DefaultSentryAuthorizationValidator.class, SentryAuthorizationValidator.class);

    if (clazz == null) {
      // should not happen as default value is set
      throw new HiveAuthzPluginException("Configuration value " + name
          + " is not set to valid SentryAuthorizationValidator subclass");
    }

    SentryAuthorizationValidator authzController = null;
    try {
      Constructor<? extends SentryAuthorizationValidator> constructor =
          clazz.getConstructor(HiveConf.class, HiveAuthzConf.class, HiveAuthenticationProvider.class);
      authzController = (SentryAuthorizationValidator)
          constructor.newInstance(conf, authzConf, authenticator);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }

    return authzController;
  }
}
