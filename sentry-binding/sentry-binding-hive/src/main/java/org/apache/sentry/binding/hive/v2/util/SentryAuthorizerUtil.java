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
package org.apache.sentry.binding.hive.v2.util;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;

public class SentryAuthorizerUtil {
  public static String UNKONWN_GRANTOR = "--";

  /**
   * Convert string to URI
   *
   * @param uri
   * @throws SemanticException
   * @throws URISyntaxException
   */
  public static AccessURI parseURI(String uri) throws URISyntaxException {
    return parseURI(uri, false);
  }

  /**
   * Convert string to URI
   *
   * @param uri
   * @param isLocal
   * @throws SemanticException
   * @throws URISyntaxException
   */
  public static AccessURI parseURI(String uri, boolean isLocal) throws URISyntaxException {
    HiveConf conf = SessionState.get().getConf();
    String warehouseDir = conf.getVar(ConfVars.METASTOREWAREHOUSE);
    return new AccessURI(PathUtils.parseDFSURI(warehouseDir, uri, isLocal));
  }

  /**
   * Convert HivePrivilegeObject to DBModelAuthorizable list
   * Now hive 0.13 don't support column level
   *
   * @param server
   * @param privilege
   */
  public static List<DBModelAuthorizable> convert2SentryPrivilege(
      Server server, HivePrivilegeObject privilege) {
    List<DBModelAuthorizable> objectHierarchy = new ArrayList<DBModelAuthorizable>();
    // first add server authorizable
    objectHierarchy.add(server);
    switch (privilege.getType()) {
      case DATABASE:
        objectHierarchy.add(new Database(privilege.getDbname()));
        break;
      case TABLE_OR_VIEW:
        objectHierarchy.add(new Database(privilege.getDbname()));
        objectHierarchy.add(new Table(privilege.getTableViewURI()));
        break;
      case LOCAL_URI:
      case DFS_URI:
        if (privilege.getTableViewURI() == null) {
          break;
        }
        try {
          objectHierarchy.add(parseURI(privilege.getTableViewURI()));
        } catch (Exception e) {
          throw new AuthorizationException("Failed to get File URI", e);
        }
        break;
      case PARTITION:
        break;
      default:
        break;
    }
    return objectHierarchy;
  }

  /**
   * Convert HivePrivilegeObject list to List<List<DBModelAuthorizable>>
   *
   * @param server
   * @param privilges
   */
  public static List<List<DBModelAuthorizable>> convert2SentryPrivilegeList(
      Server server, List<HivePrivilegeObject> privilges) {
    List<List<DBModelAuthorizable>> hierarchyList = new ArrayList<List<DBModelAuthorizable>>();
    if (privilges != null && !privilges.isEmpty()) {
      for (HivePrivilegeObject p : privilges) {
        hierarchyList.add(convert2SentryPrivilege(server, p));
      }
    }
    return hierarchyList;
  }

  /**
   * Convert HiveOperationType to HiveOperation
   *
   * @param type
   */
  public static HiveOperation convert2HiveOperation(HiveOperationType type) {
    return HiveOperation.valueOf(type.name());
  }

  /**
   * Convert HivePrivilege to Sentry Action
   *
   * @param hivePrivilege
   */
  public static String convert2SentryAction(HivePrivilege hivePrivilege) {
    if (PrivilegeType.ALL.name().equals(hivePrivilege.getName())) {
      return AccessConstants.ALL;
    } else {
      return hivePrivilege.getName();
    }
  }

  /**
   * Convert Sentry Action to HivePrivilege
   *
   * @param hivePrivilege
   */
  public static HivePrivilege convert2HivePrivilege(String action) {
    if (AccessConstants.ALL.equals(action)) {
      return new HivePrivilege(PrivilegeType.ALL.name(), null);
    } else {
      return new HivePrivilege(action, null);
    }
  }

  /**
   * Convert TSentryRole Set to String List
   *
   * @param roleSet
   */
  public static List<String> convert2RoleList(Set<TSentryRole> roleSet) {
    List<String> roles = new ArrayList<String>();
    if (roleSet != null && !roleSet.isEmpty()) {
      for (TSentryRole tRole : roleSet) {
        roles.add(tRole.getRoleName());
      }
    }
    return roles;
  }

  /**
   * Convert TSentryPrivilege to HivePrivilegeInfo
   *
   * @param tPrivilege
   * @param principal
   */
  public static HivePrivilegeInfo convert2HivePrivilegeInfo(TSentryPrivilege tPrivilege,
      HivePrincipal principal) {
      HivePrivilege hivePrivilege = convert2HivePrivilege(tPrivilege.getAction());
      HivePrivilegeObject hivePrivilegeObject = convert2HivePrivilegeObject(tPrivilege);
      // now sentry don't show grantor of a privilege
      HivePrincipal grantor = new HivePrincipal(UNKONWN_GRANTOR, HivePrincipalType.ROLE);
      boolean grantOption = tPrivilege.getGrantOption().equals(TSentryGrantOption.TRUE) ? true : false;
      return new HivePrivilegeInfo(principal, hivePrivilege, hivePrivilegeObject,
          grantor, grantOption, (int) tPrivilege.getCreateTime());
  }

  /**
   * Convert TSentryPrivilege to HivePrivilegeObject
   *
   * @param tSentryPrivilege
   */
  public static HivePrivilegeObject convert2HivePrivilegeObject(TSentryPrivilege tSentryPrivilege) {
    HivePrivilegeObject privilege = null;
    switch (PrivilegeScope.valueOf(tSentryPrivilege.getPrivilegeScope())) {
      case SERVER:
        privilege = new HivePrivilegeObject(HivePrivilegeObjectType.DFS_URI, null, null);
        break;
      case DATABASE:
        privilege = new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE,
            tSentryPrivilege.getDbName(), null);
        break;
      case TABLE:
        privilege = new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
            tSentryPrivilege.getDbName(), tSentryPrivilege.getTableName());
        break;
      case URI:
        try {
          URI uri = new URI(tSentryPrivilege.getURI());
          if (uri != null) {
            if (uri.getScheme().equalsIgnoreCase("file")) {
              privilege = new HivePrivilegeObject(HivePrivilegeObjectType.LOCAL_URI,
                  null, tSentryPrivilege.getURI());
            } else {
              privilege = new HivePrivilegeObject(HivePrivilegeObjectType.DFS_URI,
                  null, tSentryPrivilege.getURI());
            }
          }
        } catch (URISyntaxException e) {
          throw new RuntimeException(tSentryPrivilege.getURI() + "is not a URI");
        }
      default:
        break;
    }
    return privilege;
  }

  /**
   * Convert TSentryRole to HiveRoleGrant
   *
   * @param role
   */
  public static HiveRoleGrant convert2HiveRoleGrant(TSentryRole role) {
    HiveRoleGrant hiveRoleGrant = new HiveRoleGrant();
    hiveRoleGrant.setRoleName(role.getRoleName());
    hiveRoleGrant.setPrincipalName(role.getRoleName());
    hiveRoleGrant.setPrincipalType(PrincipalType.ROLE.name());
    hiveRoleGrant.setGrantOption(false);
    hiveRoleGrant.setGrantor(role.getGrantorPrincipal());
    hiveRoleGrant.setGrantorType(PrincipalType.USER.name());
    return hiveRoleGrant;
  }

  /**
   * Load HiveAuthzConf from configuration
   *
   * @param hiveConf
   */
  public static HiveAuthzConf loadAuthzConf(HiveConf hiveConf) {
    boolean depreicatedConfigFile = false;
    HiveAuthzConf newAuthzConf = null;
    String hiveAuthzConf = hiveConf.get(HiveAuthzConf.HIVE_SENTRY_CONF_URL);
    if(hiveAuthzConf == null || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
      hiveAuthzConf = hiveConf.get(HiveAuthzConf.HIVE_ACCESS_CONF_URL);
      depreicatedConfigFile = true;
    }

    if(hiveAuthzConf == null || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
      throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_SENTRY_CONF_URL
          + " value '" + hiveAuthzConf + "' is invalid.");
    }
    try {
      newAuthzConf = new HiveAuthzConf(new URL(hiveAuthzConf));
    } catch (MalformedURLException e) {
      if (depreicatedConfigFile) {
        throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_ACCESS_CONF_URL
            + " specifies a malformed URL '" + hiveAuthzConf + "'", e);
      } else {
        throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_SENTRY_CONF_URL
            + " specifies a malformed URL '" + hiveAuthzConf + "'", e);
      }
    }
    return newAuthzConf;
  }

}