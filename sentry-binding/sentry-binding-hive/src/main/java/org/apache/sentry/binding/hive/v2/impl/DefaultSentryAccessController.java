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
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.sentry.binding.hive.v2.SentryAccessController;

public class DefaultSentryAccessController extends SentryAccessController {
  public static final Log LOG = LogFactory.getLog(DefaultSentryAccessController.class);

  public DefaultSentryAccessController(HiveConf conf,
      HiveAuthenticationProvider authenticator) {
    super(conf, authenticator);
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) {
    // TODO Auto-generated method stub

  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) {
    // TODO Auto-generated method stub

  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor) {
    // TODO Auto-generated method stub

  }

  @Override
  public void dropRole(String roleName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) {
    // TODO Auto-generated method stub

  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals,
      List<String> roles, boolean grantOption, HivePrincipal grantorPrinc) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<String> getAllRoles() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
      HivePrivilegeObject privObj) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setCurrentRole(String roleName) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<String> getCurrentRoleNames() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(
      HivePrincipal principal) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) {
    // TODO Auto-generated method stub

  }

}
