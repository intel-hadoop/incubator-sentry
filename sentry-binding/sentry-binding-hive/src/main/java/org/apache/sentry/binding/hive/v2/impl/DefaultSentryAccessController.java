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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.SentryHiveConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.HiveAuthzBindingPreExecHook;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.SentryAccessController;
import org.apache.sentry.binding.hive.v2.util.SentryAuthorizerUtil;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.PrivilegeInfo;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class DefaultSentryAccessController extends SentryAccessController {
  public static final Log LOG = LogFactory.getLog(DefaultSentryAccessController.class);
  public static final String SCRATCH_DIR_PERMISSIONS = "700";
  public static final String ACCESS_RESTRICT_LIST = Joiner.on(",").join(
    ConfVars.PREEXECHOOKS.varname,
    ConfVars.SCRATCHDIR.varname,
    ConfVars.LOCALSCRATCHDIR.varname,
    ConfVars.METASTOREURIS.varname,
    ConfVars.METASTORECONNECTURLKEY.varname,
    ConfVars.HADOOPBIN.varname,
    ConfVars.HIVESESSIONID.varname,
    ConfVars.HIVEAUXJARS.varname,
    ConfVars.HIVESTATSDBCONNECTIONSTRING.varname,
    ConfVars.SCRATCHDIRPERMISSION.varname,
    ConfVars.HIVE_SECURITY_COMMAND_WHITELIST.varname,
    ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname,
    HiveAuthzConf.HIVE_ACCESS_CONF_URL,
    HiveAuthzConf.HIVE_SENTRY_CONF_URL,
    HiveAuthzConf.HIVE_ACCESS_SUBJECT_NAME,
    HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME,
    HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET
    );

  public DefaultSentryAccessController(HiveAuthzConf authzConf,
      HiveAuthzBinding hiveAuthzBinding,
      HiveAuthenticationProvider authenticator) throws Exception {
    super(authzConf, hiveAuthzBinding, authenticator);
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) {
    grantOrRevokePrivlege(hivePrincipals, hivePrivileges, hivePrivObject, grantorPrincipal,
        grantOption, true);
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) {
    grantOrRevokePrivlege(hivePrincipals, hivePrivileges, hivePrivObject, grantorPrincipal,
        grantOption, false);
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor) {
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      sentryClient.createRole(currentUserName, roleName);
    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

  @Override
  public void dropRole(String roleName) {
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      sentryClient.dropRole(currentUserName, roleName);
    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) {
    grantOrRevokeRole(hivePrincipals, roles, grantOption, grantorPrinc, true);
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals,
      List<String> roles, boolean grantOption, HivePrincipal grantorPrinc) {
    grantOrRevokeRole(hivePrincipals, roles, grantOption, grantorPrinc, false);
  }

  @Override
  public List<String> getAllRoles() {
    List<String> roles = new ArrayList<String>();
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      roles = SentryAuthorizerUtil.convert2RoleList(sentryClient.listRoles(currentUserName));
    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
    return roles;
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
      HivePrivilegeObject privObj) {
    List<HivePrivilegeInfo> infoList = new ArrayList<HivePrivilegeInfo>();
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      List<? extends Authorizable> authorizable =
          SentryAuthorizerUtil.convert2SentryPrivilege(new Server(serverName), privObj);
      Set<TSentryPrivilege> tPrivilges =
          sentryClient.listPrivilegesByRoleName(currentUserName, principal.getName(), authorizable);
      if (tPrivilges != null && !tPrivilges.isEmpty()) {
        for (TSentryPrivilege privilege : tPrivilges) {
          infoList.add(SentryAuthorizerUtil.convert2HivePrivilegeInfo(privilege, principal));
        }
      }
    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
    return infoList;
  }

  @Override
  public void setCurrentRole(String roleName) {
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      hiveAuthzBinding.setActiveRoleSet(roleName, sentryClient.listUserRoles(currentUserName));
    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

  @Override
  public List<String> getCurrentRoleNames() {
    List<String> roles = new ArrayList<String>();
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      roles = SentryAuthorizerUtil.convert2RoleList(sentryClient.listUserRoles(currentUserName));
    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
    return roles;
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName) {
    // TODO we will support in future
    throw new RuntimeException("Not supported of SHOW_ROLE_PRINCIPALS in Sentry");
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(
      HivePrincipal principal) {
    List<HiveRoleGrant> hiveRoleGrants = new ArrayList<HiveRoleGrant>();
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      HivePrincipalType principalType = principal.getType();
      // TODO it will be HivePrincipalType.GROUP in future hive version
      if (principalType != HivePrincipalType.UNKNOWN) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principalType;
        throw new RuntimeException(msg);
      }
      Set<TSentryRole> roles = sentryClient.listRolesByGroupName(currentUserName, principal.getName());
      if (roles != null && !roles.isEmpty()) {
        for (TSentryRole role : roles) {
          hiveRoleGrants.add(SentryAuthorizerUtil.convert2HiveRoleGrant(role));
        }
      }
    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
    return hiveRoleGrants;
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) {
    // grant all privileges for table to its owner
    hiveConf.setVar(ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS, "INSERT,SELECT,UPDATE,DELETE");

    // Configure PREEXECHOOKS with HiveAuthzBindingPreExecHook
    appendConfVar(hiveConf, ConfVars.PREEXECHOOKS.varname, HiveAuthzBindingPreExecHook.class.getName());

    // set security command list to only allow set command
    hiveConf.setVar(ConfVars.HIVE_SECURITY_COMMAND_WHITELIST, "set");

    // set additional configuration properties required for auth
    hiveConf.setVar(ConfVars.SCRATCHDIRPERMISSION, SCRATCH_DIR_PERMISSIONS);

    // set user name
    hiveConf.set(HiveAuthzConf.HIVE_ACCESS_SUBJECT_NAME, currentUserName);
    hiveConf.set(HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME, currentUserName);

    // set MR ACLs to session user
    appendConfVar(hiveConf, JobContext.JOB_ACL_VIEW_JOB, currentUserName);
    appendConfVar(hiveConf, JobContext.JOB_ACL_MODIFY_JOB, currentUserName);

    // setup restrict list
    hiveConf.addToRestrictList(ACCESS_RESTRICT_LIST);

  }

 //Setup given sentry hooks
 private void appendConfVar(HiveConf sessionConf, String confVar,
     String sentryConfVal) {
   String currentValue = sessionConf.get(confVar, "").trim();
   if (currentValue.isEmpty()) {
     currentValue = sentryConfVal;
   } else {
     currentValue = sentryConfVal + "," + currentValue;
   }
   sessionConf.set(confVar, currentValue);
 }

  /**
   * Grant(isGrant is true) or revoke(isGrant is false) db privileges to/from role
   * via sentryClient, which is a instance of SentryPolicyServiceClientV2
   *
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @param isGrant
   */
  private void grantOrRevokePrivlege(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption, boolean isGrant) {
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }

      for (HivePrincipal principal : hivePrincipals) {
        for (HivePrivilege privilege : hivePrivileges) {
          String grantorName = grantorPrincipal.getName();
          String roleName = principal.getName();
          String action = SentryAuthorizerUtil.convert2SentryAction(privilege);
          Boolean grantOp = null;
          if (isGrant) {
            grantOp = grantOption;
          }

          // Build privInfo by hivePrivObject's type
          PrivilegeInfo.Builder privBuilder = new PrivilegeInfo.Builder();
          PrivilegeInfo privInfo = null;
          switch (hivePrivObject.getType()) {
            case DATABASE:
              privInfo = privBuilder
                  .setPrivilegeScope(PrivilegeScope.DATABASE.toString())
                  .setServerName(serverName)
                  .setDbName(hivePrivObject.getDbname()).setAction(action)
                  .setGrantOption(grantOp)
                  .build();
              break;
            case TABLE_OR_VIEW:
              privInfo = privBuilder
                  .setPrivilegeScope(PrivilegeScope.TABLE.toString())
                  .setServerName(serverName)
                  .setDbName(hivePrivObject.getDbname())
                  .setDbName(hivePrivObject.getTableViewURI()).setAction(action)
                  .setGrantOption(grantOp)
                  .build();
              break;
            case LOCAL_URI:
            case DFS_URI:
              privInfo = privBuilder
                  .setPrivilegeScope(PrivilegeScope.URI.toString())
                  .setServerName(serverName)
                  .setDbName(hivePrivObject.getTableViewURI()).setAction(action)
                  .setGrantOption(grantOp)
                  .build();
              break;
            case PARTITION:
              break;
            default:
              privInfo = privBuilder
                  .setPrivilegeScope(PrivilegeScope.SERVER.toString())
                  .setServerName(serverName).setAction(action)
                  .setGrantOption(grantOp)
                  .build();
              break;
          }

          // Now we don't support partition
          if (privInfo == null) {
            throw new RuntimeException(hivePrivObject.getType().name() + "are not supported in sentry");
          }

          // grant or revoke privilege
          if (isGrant) {
            sentryClient.grantPrivilege(grantorName, roleName, privInfo);
          } else {
            sentryClient.revokePrivilege(grantorName, roleName, privInfo);
          }
        }
      }
    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

  /**
   * Grant(isGrant is true) or revoke(isGrant is false) role to/from group
   * via sentryClient, which is a instance of SentryPolicyServiceClientV2
   *
   * @param hivePrincipals
   * @param roles
   * @param grantOption
   * @param grantorPrinc
   * @param isGrant
   */
  private void grantOrRevokeRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc, boolean isGrant) {
    try {
      try {
        Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
        this.sentryClient = sentryClientFactory.createV2(authzConf);
      } catch (Exception e) {
        String msg = "Error creating Sentry client V2: " + e.getMessage();
        throw new RuntimeException(msg, e);
      }
      for (HivePrincipal principal : hivePrincipals) {
        for (String roleName : roles) {
          if (isGrant) {
            sentryClient.grantRoleToGroup(grantorPrinc.getName(), principal.getName(), roleName);
          } else {
            sentryClient.revokeRoleFromGroup(grantorPrinc.getName(), principal.getName(), roleName);
          }
        }
      }

    } catch(SentryUserException e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing Sentry command: " + e.getMessage();
      LOG.error(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

}
