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
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestSentryHiveAuthorizerFactory {
  private static boolean accessFlag = false;
  private static boolean authzFlag = false;

  private static HiveConf conf;
  private static final String RESOURCE_PATH = "test-authz-provider.ini";
  private HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-deprecated-site.xml"));
  private File baseDir;

  @BeforeClass
  public static void setupTestURI() {
    conf = new HiveConf();
    SessionState.start(conf);
  }

  @Before
  public void setUp() throws Exception {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);

    // create auth configuration
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
        new File(baseDir, RESOURCE_PATH).getPath());
    authzConf.set(AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), "server1");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "true");
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  static class DummySentryAccessController extends SentryAccessController {

    public DummySentryAccessController(HiveConf conf, HiveAuthzConf authzConf,
        HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) throws Exception {
      super(conf, authzConf, authenticator, ctx);
    }

    @Override
    public void grantPrivileges(List<HivePrincipal> hivePrincipals,
        List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
        HivePrincipal grantorPrincipal, boolean grantOption)
        throws SentryAccessControlException {
      accessFlag = true;
    }

    @Override
    public void revokePrivileges(List<HivePrincipal> hivePrincipals,
        List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
        HivePrincipal grantorPrincipal, boolean grantOption)
        throws SentryAccessControlException {
    }

    @Override
    public void createRole(String roleName, HivePrincipal adminGrantor)
        throws SentryAccessControlException {
    }

    @Override
    public void dropRole(String roleName) throws SentryAccessControlException {
    }

    @Override
    public void grantRole(List<HivePrincipal> hivePrincipals,
        List<String> roles, boolean grantOption, HivePrincipal grantorPrinc)
        throws SentryAccessControlException {
    }

    @Override
    public void revokeRole(List<HivePrincipal> hivePrincipals,
        List<String> roles, boolean grantOption, HivePrincipal grantorPrinc)
        throws SentryAccessControlException {
    }

    @Override
    public List<String> getAllRoles() throws SentryAccessControlException {
      return null;
    }

    @Override
    public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
        HivePrivilegeObject privObj) throws SentryAccessControlException {
      return null;
    }

    @Override
    public void setCurrentRole(String roleName)
        throws SentryAccessControlException {
    }

    @Override
    public List<String> getCurrentRoleNames() {
      return null;
    }

    @Override
    public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName) {
      return null;
    }

    @Override
    public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(
        HivePrincipal principal) {
      return null;
    }

    @Override
    public void applyAuthorizationConfigPolicy(HiveConf hiveConf) {
    }
  }

  static class DummySentryAuthorizationValidator extends SentryAuthorizationValidator {

    public DummySentryAuthorizationValidator(HiveConf conf, HiveAuthzConf authzConf,
        HiveAuthenticationProvider authenticator) throws Exception {
      super(conf, authzConf, authenticator);
    }

    @Override
    public void checkPrivileges(HiveOperationType hiveOpType,
        List<HivePrivilegeObject> inputHObjs,
        List<HivePrivilegeObject> outputHObjs,
        HiveAuthzContext context)
        throws SentryAccessControlException {
      authzFlag = true;
    }

    @Override
    public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs,
        HiveAuthzContext context) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  @Test
  public void testCreateHiveAuthorizer() throws Exception {
    conf.set(SentryAuthorizerFactory.HIVE_SENTRY_ACCESS_CONTROLLER,
        DummySentryAccessController.class.getName());
    conf.set(SentryAuthorizerFactory.HIVE_SENTRY_AUTHORIZATION_CONTROLLER,
        DummySentryAuthorizationValidator.class.getName());
    HiveAuthenticationProvider authenticator = new HadoopDefaultAuthenticator();
    SentryAuthorizerFactory factory = new SentryAuthorizerFactory();
    HiveAuthzSessionContext.Builder ctxbBuilder = new HiveAuthzSessionContext.Builder();
    HiveAuthzSessionContext ctx = ctxbBuilder.build();
    HiveAuthorizer authorizer = factory.createHiveAuthorizer(null, conf, authzConf, authenticator, ctx);
    Assert.assertFalse(accessFlag);
    authorizer.grantPrivileges(null, null, null, null, true);
    Assert.assertTrue(accessFlag);

    Assert.assertFalse(authzFlag);
    authorizer.checkPrivileges(null, null, null, null);
    Assert.assertTrue(authzFlag);
  }
}
