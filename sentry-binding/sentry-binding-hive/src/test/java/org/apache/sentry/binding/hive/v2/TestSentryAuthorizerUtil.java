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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
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
import org.apache.sentry.binding.hive.v2.util.SentryAuthorizerUtil;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSentryAuthorizerUtil {
  private static HiveConf conf;

  @BeforeClass
  public static void setupTestURI() {
    conf = new HiveConf();
    SessionState.start(conf);
  }

  @Test
  public void testParseURIIncorrectFilePrefix() throws URISyntaxException {
    Assert.assertEquals("file:///some/path",
        SentryAuthorizerUtil.parseURI("file:/some/path").getName());
  }

  @Test
  public void testParseURICorrectFilePrefix() throws URISyntaxException {
    Assert.assertEquals("file:///some/path",
        SentryAuthorizerUtil.parseURI("file:///some/path").getName());
  }

  @Test
  public void testParseURINoFilePrefix() throws URISyntaxException {
    conf.set(ConfVars.METASTOREWAREHOUSE.varname, "file:///path/to/warehouse");
    Assert.assertEquals("file:///some/path",
        SentryAuthorizerUtil.parseURI("/some/path").getName());
  }

  @Test
  public void testParseURINoHDFSPrefix() throws URISyntaxException {
    conf.set(ConfVars.METASTOREWAREHOUSE.varname, "hdfs://namenode:8080/path/to/warehouse");
    Assert.assertEquals("hdfs://namenode:8080/some/path",
        SentryAuthorizerUtil.parseURI("/some/path").getName());
  }

  @Test
  public void testParseURICorrectHDFSPrefix() throws URISyntaxException {
    Assert.assertEquals("hdfs:///some/path",
        SentryAuthorizerUtil.parseURI("hdfs:///some/path").getName());
  }

  @Test
  public void testConvertDbObject2SentryPrivilege() throws Exception {
    Server server = new Server("hs2");
    HivePrivilegeObject privilege = new HivePrivilegeObject(
        HivePrivilegeObjectType.DATABASE, "db1", null);
    List<DBModelAuthorizable> hierarchy =
        SentryAuthorizerUtil.convert2SentryPrivilege(server, privilege);
    Assert.assertNotNull(hierarchy);
    Assert.assertTrue(hierarchy.size() == 2);
    Assert.assertEquals(hierarchy.get(0).getName(), "hs2");
    Assert.assertEquals(hierarchy.get(1).getName(), "db1");
  }

  @Test
  public void testConvertTableObject2SentryPrivilege() throws Exception {
    Server server = new Server("hs2");
    HivePrivilegeObject privilege = new HivePrivilegeObject(
        HivePrivilegeObjectType.TABLE_OR_VIEW, "db1", "tb1");
    List<DBModelAuthorizable> hierarchy =
        SentryAuthorizerUtil.convert2SentryPrivilege(server, privilege);
    Assert.assertNotNull(hierarchy);
    Assert.assertTrue(hierarchy.size() == 3);
    Assert.assertEquals(hierarchy.get(0).getName(), "hs2");
    Assert.assertEquals(hierarchy.get(1).getName(), "db1");
    Assert.assertEquals(hierarchy.get(2).getName(), "tb1");
  }

  @Test
  public void testConvertLocalUriObject2SentryPrivilege() throws Exception {
    Server server = new Server("hs2");
    HivePrivilegeObject privilege = new HivePrivilegeObject(
        HivePrivilegeObjectType.LOCAL_URI, null, "file://path/to/file");
    List<DBModelAuthorizable> hierarchy =
        SentryAuthorizerUtil.convert2SentryPrivilege(server, privilege);
    Assert.assertNotNull(hierarchy);
    Assert.assertTrue(hierarchy.size() == 2);
    Assert.assertEquals(hierarchy.get(0).getName(), "hs2");
    Assert.assertEquals(hierarchy.get(1).getName(), "file://path/to/file");
  }

  @Test
  public void testConvertDfsUriObject2SentryPrivilege() throws Exception {
    Server server = new Server("hs2");
    HivePrivilegeObject privilege = new HivePrivilegeObject(
        HivePrivilegeObjectType.DFS_URI, null, "hdfs://path/to/file");
    List<DBModelAuthorizable> hierarchy =
        SentryAuthorizerUtil.convert2SentryPrivilege(server, privilege);
    Assert.assertNotNull(hierarchy);
    Assert.assertTrue(hierarchy.size() == 2);
    Assert.assertEquals(hierarchy.get(0).getName(), "hs2");
    Assert.assertEquals(hierarchy.get(1).getName(), "hdfs://path/to/file");
  }

  @Test
  public void testConvert2SentryPrivilegeList() throws Exception {
    Server server = new Server("hs2");
    List<HivePrivilegeObject> privilegeObjects = new ArrayList<HivePrivilegeObject>();
    privilegeObjects.add(new HivePrivilegeObject(
        HivePrivilegeObjectType.DATABASE, "db1", null));
    privilegeObjects.add(new HivePrivilegeObject(
        HivePrivilegeObjectType.TABLE_OR_VIEW, "db1", "tb1"));
    privilegeObjects.add(new HivePrivilegeObject(
        HivePrivilegeObjectType.LOCAL_URI, null, "file://path/to/file"));
    privilegeObjects.add(new HivePrivilegeObject(
        HivePrivilegeObjectType.DFS_URI, null, "hdfs://path/to/file"));
    List<List<DBModelAuthorizable>> hierarchyList =
        SentryAuthorizerUtil.convert2SentryPrivilegeList(server, privilegeObjects);
    Assert.assertNotNull(hierarchyList);
    Assert.assertTrue(hierarchyList.size() == 4);
    Assert.assertTrue(hierarchyList.get(0).size() == 2);
    Assert.assertEquals(hierarchyList.get(0).get(1).getName(), "db1");
    Assert.assertTrue(hierarchyList.get(1).size() == 3);
    Assert.assertEquals(hierarchyList.get(1).get(2).getName(), "tb1");
    Assert.assertTrue(hierarchyList.get(2).size() == 2);
    Assert.assertEquals(hierarchyList.get(2).get(1).getName(), "file://path/to/file");
    Assert.assertTrue(hierarchyList.get(3).size() == 2);
    Assert.assertEquals(hierarchyList.get(3).get(1).getName(), "hdfs://path/to/file");
  }

  @Test
  public void testConvert2HiveOperation() throws Exception {
    HiveOperationType type = HiveOperationType.CREATETABLE;
    HiveOperation hiveOp = SentryAuthorizerUtil.convert2HiveOperation(type);
    Assert.assertEquals(HiveOperation.CREATETABLE, hiveOp);
  }

  @Test
  public void testConvert2SentryAction() throws Exception {
    HivePrivilege hivePrivilege = new HivePrivilege(PrivilegeType.ALL.toString(), null);
    String sentryAction = SentryAuthorizerUtil.convert2SentryAction(hivePrivilege);
    Assert.assertEquals(sentryAction, AccessConstants.ALL);

    hivePrivilege = new HivePrivilege(PrivilegeType.INSERT.toString(), null);
    sentryAction = SentryAuthorizerUtil.convert2SentryAction(hivePrivilege);
    Assert.assertEquals(sentryAction, AccessConstants.INSERT.toUpperCase(Locale.US));

    hivePrivilege = new HivePrivilege(PrivilegeType.SELECT.toString(), null);
    sentryAction = SentryAuthorizerUtil.convert2SentryAction(hivePrivilege);
    Assert.assertEquals(sentryAction, AccessConstants.SELECT.toUpperCase(Locale.US));

    hivePrivilege = new HivePrivilege(PrivilegeType.ALTER_METADATA.toString(), null);
    sentryAction = SentryAuthorizerUtil.convert2SentryAction(hivePrivilege);
    Assert.assertEquals(sentryAction, AccessConstants.ALTER.toUpperCase(Locale.US));
  }

  @Test
  public void testConvert2HivePrivilege() throws Exception {
    String sentryAction = AccessConstants.ALL;
    HivePrivilege hivePrivilege = SentryAuthorizerUtil.convert2HivePrivilege(sentryAction);
    Assert.assertEquals(hivePrivilege.getName(), PrivilegeType.ALL.toString().toUpperCase(Locale.US));

    sentryAction = AccessConstants.INSERT;
    hivePrivilege = SentryAuthorizerUtil.convert2HivePrivilege(sentryAction);
    Assert.assertEquals(hivePrivilege.getName(), PrivilegeType.INSERT.toString().toUpperCase(Locale.US));

    sentryAction = AccessConstants.SELECT;
    hivePrivilege = SentryAuthorizerUtil.convert2HivePrivilege(sentryAction);
    Assert.assertEquals(hivePrivilege.getName(), PrivilegeType.SELECT.toString().toUpperCase(Locale.US));

    sentryAction = AccessConstants.ALTER;
    hivePrivilege = SentryAuthorizerUtil.convert2HivePrivilege(sentryAction);
    Assert.assertEquals(hivePrivilege.getName(),
        PrivilegeType.ALTER_METADATA.toString().toUpperCase(Locale.US));
  }

  @Test
  public void testConvert2HivePrivilegeObject() throws Exception {
    String server = "server1";
    String database = "db1";
    String table = "tb1";
    String localUri = "file://path/to/file1";
    String dfsUri = "hdfs://path/to/file2";
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(PrivilegeScope.DATABASE.name());
    privilege.setServerName(server);
    privilege.setDbName(database);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    HivePrivilegeObject hivePrivilegeObject = SentryAuthorizerUtil.convert2HivePrivilegeObject(privilege);
    Assert.assertEquals(hivePrivilegeObject.getType(), HivePrivilegeObjectType.DATABASE);
    Assert.assertEquals(hivePrivilegeObject.getDbname(), database);
    Assert.assertNull(hivePrivilegeObject.getTableViewURI());

    privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(PrivilegeScope.TABLE.name());
    privilege.setServerName(server);
    privilege.setDbName(database);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.SELECT);
    privilege.setCreateTime(System.currentTimeMillis());
    hivePrivilegeObject = SentryAuthorizerUtil.convert2HivePrivilegeObject(privilege);
    Assert.assertEquals(hivePrivilegeObject.getType(), HivePrivilegeObjectType.TABLE_OR_VIEW);
    Assert.assertEquals(hivePrivilegeObject.getDbname(), database);
    Assert.assertEquals(hivePrivilegeObject.getTableViewURI(), table);

    privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(PrivilegeScope.URI.name());
    privilege.setServerName(server);
    privilege.setURI(localUri);
    privilege.setAction(AccessConstants.INSERT);
    privilege.setCreateTime(System.currentTimeMillis());
    hivePrivilegeObject = SentryAuthorizerUtil.convert2HivePrivilegeObject(privilege);
    Assert.assertEquals(hivePrivilegeObject.getType(), HivePrivilegeObjectType.LOCAL_URI);
    Assert.assertEquals(hivePrivilegeObject.getTableViewURI(), localUri);

    privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(PrivilegeScope.URI.name());
    privilege.setServerName(server);
    privilege.setURI(dfsUri);
    privilege.setAction(AccessConstants.ALL);
    privilege.setCreateTime(System.currentTimeMillis());
    hivePrivilegeObject = SentryAuthorizerUtil.convert2HivePrivilegeObject(privilege);
    Assert.assertEquals(hivePrivilegeObject.getType(), HivePrivilegeObjectType.DFS_URI);
    Assert.assertEquals(hivePrivilegeObject.getTableViewURI(), dfsUri);
  }

  @Test
  public void testConvert2HivePrivilegeInfo() throws Exception {
    String server = "server1";
    String database = "db1";
    String table = "tb1";
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(PrivilegeScope.TABLE.name());
    privilege.setServerName(server);
    privilege.setDbName(database);
    privilege.setTableName(table);
    privilege.setAction(AccessConstants.SELECT);
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    HivePrincipal principal = new HivePrincipal("role1", HivePrincipalType.ROLE);
    HivePrivilegeInfo hivePrivilegeInfo = SentryAuthorizerUtil.convert2HivePrivilegeInfo(privilege, principal);
    Assert.assertEquals(hivePrivilegeInfo.getPrincipal().getName(), "role1");
    Assert.assertEquals(hivePrivilegeInfo.getPrivilege().getName(), AccessConstants.SELECT.toUpperCase(Locale.US));
    Assert.assertEquals(hivePrivilegeInfo.getGrantorPrincipal().getName(), SentryAuthorizerUtil.UNKONWN_GRANTOR);
    Assert.assertEquals(hivePrivilegeInfo.getPrivilege().getName(),
        AccessConstants.SELECT.toUpperCase(Locale.US));
    Assert.assertEquals(hivePrivilegeInfo.isGrantOption(), true);
    Assert.assertEquals(hivePrivilegeInfo.getObject().getType(), HivePrivilegeObjectType.TABLE_OR_VIEW);
    Assert.assertEquals(hivePrivilegeInfo.getObject().getDbname(), database);
    Assert.assertEquals(hivePrivilegeInfo.getObject().getTableViewURI(), table);
  }

  @Test
  public void testConvert2HiveRoleGrant() throws Exception {
    String grantor = SentryAuthorizerUtil.UNKONWN_GRANTOR;
    String roleName = "role1";
    TSentryRole role = new TSentryRole();
    role.setRoleName(roleName);
    role.setGrantorPrincipal(grantor);
    role.setGroups(null);
    HiveRoleGrant hiveRoleGrant = SentryAuthorizerUtil.convert2HiveRoleGrant(role);
    Assert.assertEquals(hiveRoleGrant.getRoleName(), "role1");
    Assert.assertEquals(hiveRoleGrant.getGrantor(), "--");
    Assert.assertEquals(hiveRoleGrant.getGrantorType(), PrincipalType.USER.name());
    Assert.assertEquals(hiveRoleGrant.isGrantOption(), false);
  }
}
