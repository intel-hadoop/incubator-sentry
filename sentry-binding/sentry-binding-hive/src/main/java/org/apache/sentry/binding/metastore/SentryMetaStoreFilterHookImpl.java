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
package org.apache.sentry.binding.metastore;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationScope;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges.HiveOperationType;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;

public class SentryMetaStoreFilterHookImpl extends DefaultMetaStoreFilterHookImpl {

  private HiveAuthzBinding hiveAuthzBinding;
  private HiveAuthzConf authzConf;
  private final HiveConf hiveConf;

  public SentryMetaStoreFilterHookImpl(HiveConf conf) {
    super(conf);
    this.hiveConf = conf;
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) {
    try {
      return filterShowDatabases(getHiveAuthzBinding(),
          dbList, HiveOperation.SHOWDATABASES, getUserName());
    } catch (Exception e) {
      throw new RuntimeException("Error getting DB list " + e.getMessage());
    }
  }

  @Override
  public List<String> filterTableNames(String dbName, List<String> tableList) {
    try {
      return filterShowTables(getHiveAuthzBinding(),
          tableList, HiveOperation.SHOWTABLES, getUserName(), dbName);
    } catch (Exception e) {
      throw new RuntimeException("Error getting Table list " + e.getMessage());
    }
  }

  private String getUserName() {
    return getConf().get(HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME);
  }

  /**
   * load Hive auth provider
   *
   * @return
   * @throws MetaException
   */
  private HiveAuthzBinding getHiveAuthzBinding() throws MetaException {
    if (hiveAuthzBinding == null) {
      String hiveAuthzConf = getConf().get(HiveAuthzConf.HIVE_SENTRY_CONF_URL);
      if (hiveAuthzConf == null
          || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
        throw new MetaException("Configuration key "
            + HiveAuthzConf.HIVE_SENTRY_CONF_URL + " value '" + hiveAuthzConf
            + "' is invalid.");
      }
      try {
        authzConf = new HiveAuthzConf(new URL(hiveAuthzConf));
      } catch (MalformedURLException e) {
        throw new MetaException("Configuration key "
            + HiveAuthzConf.HIVE_SENTRY_CONF_URL
            + " specifies a malformed URL '" + hiveAuthzConf + "' "
            + e.getMessage());
      }
      try {
        hiveAuthzBinding = new HiveAuthzBinding(
            HiveAuthzBinding.HiveHook.HiveMetaStore, getConf(), authzConf);
      } catch (Exception e) {
        throw new MetaException("Failed to load Hive binding " + e.getMessage());
      }
    }
    return hiveAuthzBinding;
  }

  private HiveConf getConf() {
    return SessionState.get().getConf();
  }

  private List<String> filterShowTables(
      HiveAuthzBinding hiveAuthzBinding, List<String> queryResult,
      HiveOperation operation, String userName, String dbName)
          throws SemanticException {
    List<String> filteredResult = new ArrayList<String>();
    Subject subject = new Subject(userName);
    HiveAuthzPrivileges tableMetaDataPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        setOperationScope(HiveOperationScope.TABLE).
        setOperationType(HiveOperationType.INFO).
        build();

    for (String tableName : queryResult) {
      // if user has privileges on table, add to filtered list, else discard
      Table table = new Table(tableName);
      Database database;
      database = new Database(dbName);

      List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
      externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
      externalAuthorizableHierarchy.add(database);
      externalAuthorizableHierarchy.add(table);
      inputHierarchy.add(externalAuthorizableHierarchy);

      try {
        hiveAuthzBinding.authorize(operation, tableMetaDataPrivilege, subject,
            inputHierarchy, outputHierarchy);
        filteredResult.add(table.getName());
      } catch (AuthorizationException e) {
        // squash the exception, user doesn't have privileges, so the table is
        // not added to
        // filtered list.
        ;
      }
    }
    return filteredResult;
  }

  private List<String> filterShowDatabases(
      HiveAuthzBinding hiveAuthzBinding, List<String> queryResult,
      HiveOperation operation, String userName) throws SemanticException {
    List<String> filteredResult = new ArrayList<String>();
    Subject subject = new Subject(userName);
    HiveAuthzPrivileges anyPrivilege = new HiveAuthzPrivileges.AuthzPrivilegeBuilder().
        addInputObjectPriviledge(AuthorizableType.Table, EnumSet.of(DBModelAction.SELECT, DBModelAction.INSERT)).
        addInputObjectPriviledge(AuthorizableType.URI, EnumSet.of(DBModelAction.SELECT)).
        setOperationScope(HiveOperationScope.CONNECT).
        setOperationType(HiveOperationType.QUERY).
        build();

    for (String dbName:queryResult) {
      // if user has privileges on database, add to filtered list, else discard
      Database database = null;

      // if default is not restricted, continue
      if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(dbName)
          && "false".equalsIgnoreCase(hiveAuthzBinding.getAuthzConf().get(
              HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(),
              "false"))) {
        filteredResult.add(DEFAULT_DATABASE_NAME);
        continue;
      }

      database = new Database(dbName);

      List<List<DBModelAuthorizable>> inputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<List<DBModelAuthorizable>> outputHierarchy = new ArrayList<List<DBModelAuthorizable>>();
      List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
      externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
      externalAuthorizableHierarchy.add(database);
      externalAuthorizableHierarchy.add(Table.ALL);
      inputHierarchy.add(externalAuthorizableHierarchy);

      try {
        hiveAuthzBinding.authorize(operation, anyPrivilege, subject,
            inputHierarchy, outputHierarchy);
        filteredResult.add(database.getName());
      } catch (AuthorizationException e) {
        // squash the exception, user doesn't have privileges, so the table is
        // not added to
        // filtered list.
        ;
      }
    }

    return filteredResult;
  }

}
