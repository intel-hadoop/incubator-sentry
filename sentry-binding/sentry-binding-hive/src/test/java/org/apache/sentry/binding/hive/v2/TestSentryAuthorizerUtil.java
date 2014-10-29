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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.v2.util.SentryAuthorizerUtil;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Server;
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
}
