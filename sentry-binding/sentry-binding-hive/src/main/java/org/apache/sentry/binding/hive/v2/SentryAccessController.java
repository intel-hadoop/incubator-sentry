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
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;

/**
 * Abstract class to do access control commands,
 * e.g. grant/revoke privileges, grant/revoke role, create/drop role.
 */
public abstract class SentryAccessController implements HiveAccessController {

  private final HiveAuthenticationProvider authenticator;
  private String currentUserName;

  public SentryAccessController(HiveConf conf, HiveAuthenticationProvider authenticator) {
    this.authenticator = authenticator;
    initUserRoles();
  }

  /**
   * (Re-)initialize currentUserName or currentRoleName if necessary.
   */
  private void initUserRoles() {
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

  @Override
  public abstract void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws SentryAccessControlException;

  @Override
  public abstract void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws SentryAccessControlException;

  @Override
  public abstract void createRole(String roleName, HivePrincipal adminGrantor)
      throws SentryAccessControlException;

  @Override
  public abstract void dropRole(String roleName) throws SentryAccessControlException;

  @Override
  public abstract void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws SentryAccessControlException;

  @Override
  public abstract void revokeRole(List<HivePrincipal> hivePrincipals,
      List<String> roles, boolean grantOption, HivePrincipal grantorPrinc)
          throws SentryAccessControlException;

  @Override
  public abstract List<String> getAllRoles() throws SentryAccessControlException;

  @Override
  public abstract List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
      HivePrivilegeObject privObj) throws SentryAccessControlException;

  @Override
  public abstract void setCurrentRole(String roleName) throws SentryAccessControlException;

  @Override
  public abstract List<String> getCurrentRoleNames() ;

  @Override
  public abstract List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName);

  @Override
  public abstract List<HiveRoleGrant> getRoleGrantInfoForPrincipal(
      HivePrincipal principal);

  @Override
  public abstract void applyAuthorizationConfigPolicy(HiveConf hiveConf);

}
