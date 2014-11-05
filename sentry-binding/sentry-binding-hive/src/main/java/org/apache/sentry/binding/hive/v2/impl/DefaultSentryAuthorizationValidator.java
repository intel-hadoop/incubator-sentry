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
package org.apache.sentry.binding.hive.v2.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding.HiveHook;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.SentryAuthorizationValidator;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;
import org.apache.sentry.binding.hive.v2.util.SentryAuthorizerUtil;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.DBModelAuthorizable;

public class DefaultSentryAuthorizationValidator extends SentryAuthorizationValidator {

  public static final Log LOG = LogFactory.getLog(DefaultSentryAuthorizationValidator.class);

  public DefaultSentryAuthorizationValidator(HiveAuthzConf authzConf, HiveAuthzBinding hiveAuthzBinding,
      HiveAuthenticationProvider authenticator) throws Exception {
    super(authzConf, hiveAuthzBinding, authenticator);
  }

  public DefaultSentryAuthorizationValidator(HiveHook hiveHook, HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    this(authzConf, new HiveAuthzBinding(hiveHook, conf, authzConf), authenticator);
  }

  public DefaultSentryAuthorizationValidator(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    this(HiveHook.HiveServer2, conf, authzConf, authenticator);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType,
      List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs) throws SentryAccessControlException {
    HiveOperation hiveOp = SentryAuthorizerUtil.convert2HiveOperation(hiveOpType);
    HiveAuthzPrivileges stmtAuthPrivileges = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(hiveOp);
    List<List<DBModelAuthorizable>> inputHierarchyList =
        SentryAuthorizerUtil.convert2SentryPrivilegeList(hiveAuthzBinding.getAuthServer(), inputHObjs);
    List<List<DBModelAuthorizable>> outputHierarchyList =
        SentryAuthorizerUtil.convert2SentryPrivilegeList(hiveAuthzBinding.getAuthServer(), outputHObjs);
    try {
      hiveAuthzBinding.authorize(hiveOp, stmtAuthPrivileges, new Subject(currentUserName),
          inputHierarchyList, outputHierarchyList);
    } catch (AuthorizationException e) {
      throw new SentryAccessControlException(e);
    }
  }

  /**
   * Set currentUserName just for testing
   */
  public void setUserName(String newUserName) {
    this.currentUserName = newUserName;
  }
}
