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

import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationValidator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;

import com.google.common.base.Preconditions;

/**
 * Abstract class used to do authorization.
 * Check if current user has privileges to do the operation.
 */
public abstract class SentryAuthorizationValidator implements HiveAuthorizationValidator {
  protected HiveAuthzBinding hiveAuthzBinding;
  protected HiveAuthzConf authzConf;
  protected HiveAuthenticationProvider authenticator;

  public SentryAuthorizationValidator(HiveAuthzConf authzConf, HiveAuthzBinding hiveAuthzBinding,
      HiveAuthenticationProvider authenticator) throws Exception {
    initilize(authzConf, hiveAuthzBinding, authenticator);
  }

  /**
   * initialize authenticator and hiveAuthzBinding.
   */
  protected void initilize(HiveAuthzConf authzConf, HiveAuthzBinding hiveAuthzBinding,
      HiveAuthenticationProvider authenticator) throws Exception {
    Preconditions.checkNotNull(authzConf, "HiveAuthzConf cannot be null");
    Preconditions.checkNotNull(hiveAuthzBinding, "HiveAuthzBinding cannot be null");
    Preconditions.checkNotNull(authenticator, "Hive authenticator provider cannot be null");
    this.authzConf = authzConf;
    this.hiveAuthzBinding = hiveAuthzBinding;
    this.authenticator = authenticator;
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
