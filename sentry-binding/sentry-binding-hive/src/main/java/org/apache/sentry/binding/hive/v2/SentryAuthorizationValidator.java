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

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationValidator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding.HiveHook;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;
import org.apache.sentry.binding.hive.v2.util.SentryAuthorizerUtil;

/**
 * Abstract class used to do authorization.
 * Check if current user has privileges to do the operation.
 */
public abstract class SentryAuthorizationValidator implements HiveAuthorizationValidator {
  protected HiveAuthzBinding hiveAuthzBinding;
  protected HiveAuthzConf authzConf;
  protected HiveAuthenticationProvider authenticator;
  protected String currentUserName;

  public SentryAuthorizationValidator(HiveHook hiveHook, HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    initilize(hiveHook, conf, authzConf, authenticator);
  }

  public SentryAuthorizationValidator(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    initilize(HiveHook.HiveServer2, conf, authzConf, authenticator);
  }

  public SentryAuthorizationValidator(HiveConf conf, HiveAuthenticationProvider authenticator)
      throws Exception {
    initilize(HiveHook.HiveServer2, conf, null, authenticator);
  }

  /**
   * initialize authenticator and hiveAuthzBinding.
   */
  protected void initilize(HiveHook hiveHook, HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    if(conf == null) {
      throw new HiveAuthzPluginException("Session HiveConf is null");
    }
    this.authenticator = authenticator;
    if (authzConf == null) {
      authzConf = SentryAuthorizerUtil.loadAuthzConf(conf);
    }
    hiveAuthzBinding = new HiveAuthzBinding(hiveHook, conf, authzConf);
    initUserRoles();
  }

  /**
   * (Re-)initialize currentUserName or currentRoleName if necessary.
   */
  protected void initUserRoles() {
    // to aid in testing through .q files, authenticator is passed as argument to
    // the interface. this helps in being able to switch the user within a session.
    // so we need to check if the user has changed
    String newUserName = authenticator.getUserName();
    if(currentUserName == newUserName){
      //no need to (re-)initialize the currentUserName, currentRoles fields
      return;
    }
    this.currentUserName = newUserName;
  }

  /**
   * Check if current user has privileges to perform given operation type
   * hiveOpType on the given input and output objects
   *
   * @param hiveOpType
   * @param inputHObjs
   * @param outputHObjs
   * @param context
   * @throws SentryAccessControlException
   */
  @Override
  public abstract void checkPrivileges(HiveOperationType hiveOpType,
      List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs) throws SentryAccessControlException;

}
