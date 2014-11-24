/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.binding.hive.v2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.binding.hive.conf.InvalidConfigurationException;
import org.apache.sentry.binding.hive.v2.impl.DefaultSentryAuthorizationValidator;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestDefaultSentryAuthorizationValidator {
  private static final String RESOURCE_PATH = "test-authz-provider.ini";

  // Servers
  private static final String SERVER1 = "server1";

  // Users
  private static final String ADMIN_USER = "admin1";
  private static final String MANAGER_USER = "manager1";
  private static final String ANALYST_USER = "analyst1";
  private static final String JUNIOR_ANALYST_USER = "junior_analyst1";
  private static final String NO_SUCH_USER = "no such subject";

  // Databases
  private static final String CUSTOMER_DB = "customers";
  private static final String ANALYST_DB = "analyst";
  private static final String JUNIOR_ANALYST_DB = "junior_analyst";

  // Tables
  private static final String PURCHASES_TAB = "purchases";
  private static final String PAYMENT_TAB = "payments";

  //Columns
  private static final String AGE_COL = "age";
  private static final String NAME_COL = "name";

  // Entities
  private List<List<DBModelAuthorizable>> inputTabHierarcyList = new ArrayList<List<DBModelAuthorizable>>();
  private List<List<DBModelAuthorizable>> outputTabHierarcyList = new ArrayList<List<DBModelAuthorizable>>();
  private static HiveConf hiveConf = new HiveConf();
  private HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-deprecated-site.xml"));

  // auth bindings handler
  private DummyHiveAuthenticationProvider authenticator = new DummyHiveAuthenticationProvider();
  private DefaultSentryAuthorizationValidator validator;

  private File baseDir;

  static class DummyHiveAuthenticationProvider implements HiveAuthenticationProvider {
    private String userName;
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public String getUserName() {
      return userName;
    }

    @Override
    public List<String> getGroupNames() {
      return null;
    }

    @Override
    public void destroy() throws HiveException {

    }

    @Override
    public void setSessionState(SessionState ss) {

    }

    public void setUserName(String user) {
      this.userName = user;
    }

  }

  @BeforeClass
  public static void setupTestURI() {
    SessionState.start(hiveConf);
  }

  @Before
  public void setUp() throws Exception {
    inputTabHierarcyList.clear();
    outputTabHierarcyList.clear();
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);

    // create auth configuration
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), new File(
        baseDir, RESOURCE_PATH).getPath());
    authzConf.set(AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), SERVER1);
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "true");
    validator = new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);
  }

  @After
  public void teardown() {
    if (baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  /**
   * validate read permission for admin on customer:purchase
   */
  @Test
  public void testValidateSelectPrivilegesForAdmin() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
        CUSTOMER_DB, PURCHASES_TAB));
    authenticator.setUserName(ADMIN_USER);
    validator.checkPrivileges(HiveOperationType.QUERY, inputHObjs, null);
  }

  /**
   * validate read permission for admin on customer:purchase
   */
  @Test
  public void testValidateSelectPrivilegesForUsers() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
        CUSTOMER_DB, PURCHASES_TAB));
    authenticator.setUserName(ANALYST_USER);
    validator.checkPrivileges(HiveOperationType.QUERY, inputHObjs, null);
  }

  /**
   * validate read permission for denied for junior analyst on customer:purchase
   */
  @Test(expected=SentryAccessControlException.class)
  public void testValidateSelectPrivilegesRejectionForUsers() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
        CUSTOMER_DB, PURCHASES_TAB));
    authenticator.setUserName(JUNIOR_ANALYST_USER);
    validator.checkPrivileges(HiveOperationType.QUERY, inputHObjs, null);
  }

  /**
   * validate create table permissions for admin in customer db
   */
  @Test
  public void testValidateCreateTabPrivilegesForAdmin() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE,
        CUSTOMER_DB, null));
    authenticator.setUserName(ADMIN_USER);
    validator.checkPrivileges(HiveOperationType.CREATETABLE, inputHObjs, null);
  }

  /**
   * validate create table permissions for manager in junior_analyst sandbox db
   */
  @Test
  public void testValidateCreateTabPrivilegesForUser() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE,
        JUNIOR_ANALYST_DB, null));
    authenticator.setUserName(MANAGER_USER);
    validator.checkPrivileges(HiveOperationType.CREATETABLE, inputHObjs, null);
  }

  /**
   * validate create table permissions denided to junior_analyst in customer db
   */
  @Test(expected=SentryAccessControlException.class)
  public void testValidateCreateTabPrivilegesRejectionForUser() throws Exception {
    List<HivePrivilegeObject> outputHObjs = new ArrayList<HivePrivilegeObject>();
    outputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE,
        CUSTOMER_DB, null));
    authenticator.setUserName(JUNIOR_ANALYST_USER);
    validator.checkPrivileges(HiveOperationType.CREATETABLE, null, outputHObjs);
  }

  /**
   * validate create table permissions denided to junior_analyst in analyst sandbox db
   */
  @Test(expected=SentryAccessControlException.class)
  public void testValidateCreateTabPrivilegesRejectionForUser2() throws Exception {
    List<HivePrivilegeObject> outputHObjs = new ArrayList<HivePrivilegeObject>();
    outputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE,
        ANALYST_DB, null));
    authenticator.setUserName(JUNIOR_ANALYST_USER);
    validator.checkPrivileges(HiveOperationType.CREATETABLE, null, outputHObjs);
  }

  /**
   * validate load permissions for admin on customer:purchases
   */
  @Test
  public void testValidateLoadTabPrivilegesForAdmin() throws Exception {
    List<HivePrivilegeObject> outputHObjs = new ArrayList<HivePrivilegeObject>();
    outputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
        CUSTOMER_DB, PURCHASES_TAB));
    authenticator.setUserName(ADMIN_USER);
    validator.checkPrivileges(HiveOperationType.LOAD, null, outputHObjs);
  }

  /**
   * validate load table permissions on manager for customer:purchases
   */
  @Test
  public void testValidateLoadTabPrivilegesForUser() throws Exception {
    List<HivePrivilegeObject> outputHObjs = new ArrayList<HivePrivilegeObject>();
    outputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
        CUSTOMER_DB, PURCHASES_TAB));
    authenticator.setUserName(MANAGER_USER);
    validator.checkPrivileges(HiveOperationType.LOAD, null, outputHObjs);
  }

  /**
   * validate load table permissions rejected for analyst on customer:purchases
   */
  @Test(expected=SentryAccessControlException.class)
  public void testValidateLoadTabPrivilegesRejectionForUser() throws Exception {
    List<HivePrivilegeObject> outputHObjs = new ArrayList<HivePrivilegeObject>();
    outputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
        CUSTOMER_DB, PURCHASES_TAB));
    authenticator.setUserName(ANALYST_USER);
    validator.checkPrivileges(HiveOperationType.LOAD, null, outputHObjs);
  }

  /**
   * validate create database permission for admin
   */
  @Test
  public void testValidateCreateDbForAdmin() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DFS_URI, null, null));
    authenticator.setUserName(ADMIN_USER);
    validator.checkPrivileges(HiveOperationType.CREATEDATABASE, inputHObjs, null);
  }

  /**
   * validate create database permission for admin
   */
  @Test(expected=SentryAccessControlException.class)
  public void testValidateCreateDbRejectionForUser() throws Exception {
    // Hive compiler doesn't capture Entities for DB operations
    List<HivePrivilegeObject> outputHObjs = new ArrayList<HivePrivilegeObject>();
    outputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DFS_URI, null, null));
    authenticator.setUserName(ANALYST_USER);
    validator.checkPrivileges(HiveOperationType.CREATEDATABASE, null, outputHObjs);
  }

  /**
   * Validate create function permission for admin (server level priviledge
   */
  @Test
  public void testValidateCreateFunctionForAdmin() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.LOCAL_URI,
        null, "file:///some/path/to/a/jar"));
    authenticator.setUserName(ADMIN_USER);
    validator.checkPrivileges(HiveOperationType.CREATEFUNCTION, inputHObjs, null);
  }

  @Test
  public void testValidateCreateFunctionAppropiateURI() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.LOCAL_URI,
        null, "file:///path/to/some/lib/dir/my.jar"));
    authenticator.setUserName(ANALYST_USER);
    validator.checkPrivileges(HiveOperationType.CREATEFUNCTION, inputHObjs, null);
  }

  @Test(expected=SentryAccessControlException.class)
  public void testValidateCreateFunctionRejectionForUnknownUser() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.LOCAL_URI,
        null, "file:///some/path/to/a/jar"));
    authenticator.setUserName(NO_SUCH_USER);
    validator.checkPrivileges(HiveOperationType.CREATEFUNCTION, inputHObjs, null);
  }

  @Test(expected=SentryAccessControlException.class)
  public void testValidateCreateFunctionRejectionForUserWithoutURI() throws Exception {
    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
        CUSTOMER_DB, AccessConstants.ALL));
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.LOCAL_URI,
        null, "file:///some/path/to/a.jar"));
    authenticator.setUserName(ANALYST_USER);
    validator.checkPrivileges(HiveOperationType.CREATEFUNCTION, inputHObjs, null);
  }

  /**
   * Turn on impersonation and make sure InvalidConfigurationException is thrown.
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testImpersonationRestriction() throws Exception {
    // perpare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "Kerberos");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    validator = new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);
  }

  /**
   * HiveServer2 not using string authentication, make sure InvalidConfigurationException is thrown.
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testHiveServer2AuthRestriction() throws Exception {
    // prepare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "none");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    validator = new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);
  }

  /**
   * hive.metastore.sasl.enabled != true, make sure InvalidConfigurationException is thrown.
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testHiveMetaStoreSSLConfig() throws Exception {
    // prepare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, false);
    hiveConf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    validator = new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);
  }

  /**
   * hive.metastore.execute.setugi != true, make sure InvalidConfigurationException is thrown.
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testHiveMetaStoreUGIConfig() throws Exception {
    // prepare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, true);
    hiveConf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI, false);
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "true");
    validator = new DefaultSentryAuthorizationValidator(HiveAuthzBinding.HiveHook.HiveMetaStore,
        hiveConf, authzConf, authenticator);
  }

  /**
   * Turn on impersonation and make sure that the authorization fails.
   * @throws Exception
   */
  @Test
  public void testImpersonationAllowed() throws Exception {
    // perpare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "Kerberos");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    authzConf.set(AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION.getVar(), "true");
    validator = new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);

    List<HivePrivilegeObject> inputHObjs = new ArrayList<HivePrivilegeObject>();
    inputHObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
        CUSTOMER_DB, PURCHASES_TAB));
    authenticator.setUserName(ADMIN_USER);
    validator.checkPrivileges(HiveOperationType.QUERY, inputHObjs, null);
  }

  /**
   * Turn off authentication and verify exception is raised in non-testing mode
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testNoAuthenticationRestriction() throws Exception {
    // perpare the hive and auth configs
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "None");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    validator = new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);
  }

  /**
   * Verify that an existing definition of only the AuthorizationProvider
   * (not ProviderBackend or PolicyEngine) still works.
   */
  @Test
  public void testDeprecatedHiveAuthzConfs() throws Exception {
    // verify that a non-existant AuthorizationProvider throws an Exception
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
      "org.apache.sentry.provider.BogusProvider");
    try {
      new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);
      Assert.fail("Expected exception");
    } catch (ClassNotFoundException e) {}

    // verify HadoopGroupResourceAuthorizationProvider
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
      "org.apache.sentry.provider.file.HadoopGroupResourceAuthorizationProvider");
    new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);

    // verify LocalGroupResourceAuthorizationProvider
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
      "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    new DefaultSentryAuthorizationValidator(hiveConf, authzConf, authenticator);
  }
}
