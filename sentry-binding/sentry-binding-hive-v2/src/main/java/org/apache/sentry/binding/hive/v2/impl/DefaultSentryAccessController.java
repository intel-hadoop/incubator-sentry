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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.SentryHiveConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.SentryOnFailureHookContext;
import org.apache.sentry.binding.hive.SentryOnFailureHookContextImpl;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding.HiveHook;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.SentryAccessController;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;
import org.apache.sentry.binding.hive.v2.util.SentryAuthorizerUtil;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.PrivilegeInfo;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class DefaultSentryAccessController extends SentryAccessController {
  public static final Logger LOG = LoggerFactory.getLogger(DefaultSentryAccessController.class);
  private HiveHook hiveHook;
  private HiveAuthzBinding hiveAuthzBinding;

  public DefaultSentryAccessController(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator,
      HiveAuthzSessionContext ctx) throws Exception {
    this(HiveHook.HiveServer2, conf, authzConf, authenticator, ctx);
  }

  public DefaultSentryAccessController(HiveHook hiveHook, HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator,
      HiveAuthzSessionContext ctx) throws Exception {
    super(conf, authzConf, authenticator, ctx);
    this.hiveHook = hiveHook;
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
      sentryClient = getSentryClient();
      sentryClient.createRole(authenticator.getUserName(), roleName);
    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.CREATEROLE;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient create role: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing CREATE ROLE command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

  @Override
  public void dropRole(String roleName) {
    try {
      sentryClient = getSentryClient();
      sentryClient.dropRole(authenticator.getUserName(), roleName);
    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.DROPROLE;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient drop role: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing DROP ROLE command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
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
      sentryClient = getSentryClient();
      roles = SentryAuthorizerUtil.convert2RoleList(sentryClient.listRoles(authenticator.getUserName()));
    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.SHOW_ROLES;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient listRoles: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing SHOW ROLES command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
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
      if (principal.getType() != HivePrincipalType.ROLE) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL +
            principal.getType();
        throw new SentryAccessControlException(msg);
      }

      sentryClient = getSentryClient();
      List<List<DBModelAuthorizable>> authorizables =
          SentryAuthorizerUtil.getAuthzHierarchy(new Server(serverName), privObj);
      Set<TSentryPrivilege> tPrivilges = new HashSet<TSentryPrivilege>();
      if (authorizables != null && !authorizables.isEmpty()) {
        for (List<? extends Authorizable> authorizable : authorizables) {
          tPrivilges.addAll(sentryClient.listPrivilegesByRoleName(authenticator.getUserName(),
              principal.getName(), authorizable));
        }
      } else {
        tPrivilges.addAll(sentryClient.listPrivilegesByRoleName(authenticator.getUserName(),
              principal.getName(), null));
      }

      if (tPrivilges != null && !tPrivilges.isEmpty()) {
        for (TSentryPrivilege privilege : tPrivilges) {
          infoList.add(SentryAuthorizerUtil.convert2HivePrivilegeInfo(privilege, principal));
        }
      }
    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.SHOW_GRANT;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient listPrivilegesByRoleName: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing SHOW GRANT command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
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
      sentryClient = getSentryClient();
      hiveAuthzBinding = new HiveAuthzBinding(hiveHook, conf, authzConf);
      hiveAuthzBinding.setActiveRoleSet(roleName, sentryClient.listUserRoles(authenticator.getUserName()));
    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.GRANT_ROLE;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient listUserRoles or hiveAuthzBinding setActiveRole" + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing SET ROLE command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
      if (hiveAuthzBinding != null) {
        hiveAuthzBinding.close();
      }
    }
  }

  @Override
  public List<String> getCurrentRoleNames() {
    List<String> roles = new ArrayList<String>();
    try {
      sentryClient = getSentryClient();
      hiveAuthzBinding = new HiveAuthzBinding(hiveHook, conf, authzConf);
      ActiveRoleSet roleSet = hiveAuthzBinding.getActiveRoleSet();
      if(roleSet.isAll()) {
        roles = SentryAuthorizerUtil.convert2RoleList(
            sentryClient.listUserRoles(authenticator.getUserName()));
      } else {
        roles.addAll(roleSet.getRoles());
      }
    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.SHOW_ROLES;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient listUserRoles: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing SHOW CURRENT ROLES command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
      if (hiveAuthzBinding != null) {
        hiveAuthzBinding.close();
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
      sentryClient = getSentryClient();

      if (principal.getType() != HivePrincipalType.GROUP) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principal.getType();
        throw new SentryAccessControlException(msg);
      }
      Set<TSentryRole> roles = sentryClient.listRolesByGroupName(
          authenticator.getUserName(), principal.getName());
      if (roles != null && !roles.isEmpty()) {
        for (TSentryRole role : roles) {
          hiveRoleGrants.add(SentryAuthorizerUtil.convert2HiveRoleGrant(role));
        }
      }
    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.SHOW_ROLE_GRANT;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient listRolesByGroupName: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } catch(Throwable e) {
      String msg = "Error processing SHOW ROLE GRANT command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
    return hiveRoleGrants;
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) {
    // Apply rest of the configuration only to HiveServer2
    if (ctx.getClientType() != CLIENT_TYPE.HIVESERVER2
        || !hiveConf.getBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      throw new RuntimeException("Sentry just support for hiveserver2");
    }
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
      sentryClient = getSentryClient();

      for (HivePrincipal principal : hivePrincipals) {
        // Now Sentry only support grant privilege to ROLE
        if (principal.getType() != HivePrincipalType.ROLE) {
          String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principal.getType();
          throw new SentryAccessControlException(msg);
        }
        for (HivePrivilege privilege : hivePrivileges) {
          String grantorName = grantorPrincipal.getName();
          String roleName = principal.getName();
          String action = SentryAuthorizerUtil.convert2SentryAction(privilege);
          List<String> columnNames = privilege.getColumns();
          Boolean grantOp = null;
          if (isGrant) {
            grantOp = grantOption;
          } else {
            // TODO
            // Now RevokeDesc has no grantor, so if this is revoke task, grantor will be null.
            // Do it here to make it workaround for SENTRY.
            grantorName = authenticator.getUserName();
          }

          // Build privInfo by hivePrivObject's type
          PrivilegeInfo.Builder privBuilder = new PrivilegeInfo.Builder();
          PrivilegeInfo privInfo = null;
          switch (hivePrivObject.getType()) {
            case GLOBAL:
              privInfo = privBuilder
                .setPrivilegeScope(PrivilegeScope.SERVER.toString())
                .setServerName(hivePrivObject.getObjectName())
                .setAction(action)
                .setGrantOption(grantOp)
                .build();
              break;
            case DATABASE:
              privInfo = privBuilder
                .setPrivilegeScope(PrivilegeScope.DATABASE.toString())
                .setServerName(serverName)
                .setDbName(hivePrivObject.getDbname())
                .setAction(action)
                .setGrantOption(grantOp)
                .build();
              break;
            case TABLE_OR_VIEW:
              privBuilder
                .setPrivilegeScope(PrivilegeScope.TABLE.toString())
                .setServerName(serverName)
                .setDbName(hivePrivObject.getDbname())
                .setTableOrViewName(hivePrivObject.getObjectName())
                .setAction(action)
                .setGrantOption(grantOp);
              // TODO workaround for column level security
              if (columnNames != null && !columnNames.isEmpty()) {
                if (action.equalsIgnoreCase(AccessConstants.INSERT) ||
                    action.equalsIgnoreCase(AccessConstants.ALL)) {
                  String msg = SentryHiveConstants.PRIVILEGE_NOT_SUPPORTED
                      + privilege.getName() + " on Column";
                  throw new SentryAccessControlException(msg);
                }
                privBuilder
                  .setPrivilegeScope(PrivilegeScope.COLUMN.toString())
                  .setColumns(columnNames);
              }
              privInfo = privBuilder.build();
              break;
            case LOCAL_URI:
            case DFS_URI:
              privInfo = privBuilder
                .setPrivilegeScope(PrivilegeScope.URI.toString())
                .setServerName(serverName)
                .setURI(hivePrivObject.getObjectName().replace("'", "").replace("\"", ""))
                // TODO In current version, URI privilege only support action of ALL
                .setAction(AccessConstants.ALL)
                .setGrantOption(grantOp)
                .build();
              break;
            case FUNCTION:
            case PARTITION:
            case COLUMN:
            case COMMAND_PARAMS:
              // not support these type
              break;
            default:
              break;
          }

          // Now we don't support PARTITION, COLUMN, FUNCTION, COMMAND_PARAMS
          if (privInfo == null) {
            throw new SentryAccessControlException(hivePrivObject.getType().name() +
                "are not supported in sentry");
          }

          // grant or revoke privilege
          if (isGrant) {
            sentryClient.grantPrivilege(grantorName, roleName, privInfo);
          } else {
            sentryClient.revokePrivilege(grantorName, roleName, privInfo);
          }
        }
      }
    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = isGrant? HiveOperation.GRANT_PRIVILEGE : HiveOperation.REVOKE_PRIVILEGE;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient grant/revoke privilege:" + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(e);
    } catch(Throwable e) {
      String msg = "Error processing GRANT/REVOKE PRIVILEGE command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(e);
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
      sentryClient = getSentryClient();
      // get principals
      Set<String> groups = Sets.newHashSet();
      for (HivePrincipal principal : hivePrincipals) {
        if (principal.getType() != HivePrincipalType.GROUP) {
          String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principal.getType();
          throw new SentryAccessControlException(msg);
        }
        groups.add(principal.getName());
      }

      // grant/revoke role to/from principals
      for (String roleName : roles) {
        if (isGrant) {
          sentryClient.grantRoleToGroups(grantorPrinc.getName(), roleName, groups);
        } else {
          sentryClient.revokeRoleFromGroups(grantorPrinc.getName(), roleName, groups);
        }
      }

    } catch(SentryAccessDeniedException e) {
      HiveOperation hiveOp = isGrant? HiveOperation.GRANT_ROLE : HiveOperation.REVOKE_ROLE;
      executeOnFailureHooks(hiveOp, e);
    } catch(SentryUserException e) {
      String msg = "Error when sentryClient grant/revoke role:" + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(e);
    } catch(Exception e) {
      String msg = "Error processing GRANT/REVOKE ROLE command: " + e.getMessage();
      LOG.error(msg, e);
      throw new RuntimeException(e);
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
      }
    }
  }

  private void executeOnFailureHooks(HiveOperation hiveOp, SentryAccessDeniedException e) {
    SentryOnFailureHookContext hookCtx = new SentryOnFailureHookContextImpl(
        SessionState.get().getCmd(), null, null,
        hiveOp, null, null, null, null, authenticator.getUserName(),
        null, new AuthorizationException(e), authzConf);
    SentryAuthorizerUtil.executeOnFailureHooks(hookCtx, authzConf);
    throw new RuntimeException(e);
  }

  private SentryPolicyServiceClient getSentryClient() throws SentryAccessControlException {
    try {
      Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
      return SentryServiceClientFactory.create(authzConf);
    } catch (Exception e) {
      String msg = "Error creating Sentry client V2: " + e.getMessage();
      throw new SentryAccessControlException(msg, e);
    }
  }
}
