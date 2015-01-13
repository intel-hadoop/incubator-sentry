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

package org.apache.sentry.service.thrift;
import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.sentry.provider.db.service.thrift.SentryMiniKdcTestcase;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperSaslServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public abstract class SentryServiceIntegrationBase extends SentryMiniKdcTestcase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceIntegrationBase.class);

  static {
    if (System.getProperty("sun.security.krb5.debug", "").trim().isEmpty()) {
      System.setProperty("sun.security.krb5.debug", String.valueOf("true"));
    }
  }

  protected static final String SERVER_HOST = NetUtils.createSocketAddr("localhost:80").getAddress().getCanonicalHostName();
  protected static final String REALM = "EXAMPLE.COM";
  protected static final String SERVER_PRINCIPAL = "sentry/" + SERVER_HOST;
  protected static final String SERVER_KERBEROS_NAME = "sentry/" + SERVER_HOST + "@" + REALM;
  protected static final String CLIENT_PRINCIPAL = "hive/" + SERVER_HOST;
  protected static final String CLIENT_KERBEROS_SHORT_NAME = "hive";
  protected static final String CLIENT_KERBEROS_NAME = CLIENT_KERBEROS_SHORT_NAME
      + "/" + SERVER_HOST + "@" + REALM;
  protected static final String ADMIN_USER = "admin_user";
  protected static final String ADMIN_GROUP = "admin_group";

  protected SentryService server;
  protected SentryServicePolicyClientFactory clientFactory;
  protected SentryPolicyServiceClient client;
  protected MiniKdc kdc;
  protected File kdcWorkDir;
  protected File dbDir;
  protected File serverKeytab;
  protected File clientKeytab;
  protected Subject clientSubject;
  protected LoginContext clientLoginContext;
  protected boolean kerberos;
  protected final Configuration conf = new Configuration(false);
  protected PolicyFile policyFile;
  protected File policyFilePath;
  protected Properties kdcConfOverlay = new Properties();

  protected boolean haEnabled = false;
  protected static final String ZK_SERVER_PRINCIPAL = "zookeeper/" + SERVER_HOST;
  protected TestingServer zkServer;

  private File ZKKeytabFile;

  protected boolean pooled = false;

  @Before
  public void setup() throws Exception {
    this.kerberos = true;
    this.pooled = true;
    beforeSetup();
    setupConf();
    startSentryService();
    connectToSentryService();
    afterSetup();
  }

  private void setupKdc() throws Exception {
    startMiniKdc(kdcConfOverlay);
  }

  public void startSentryService() throws Exception {
    server.start();
    final long start = System.currentTimeMillis();
    while(!server.isRunning()) {
      Thread.sleep(1000);
      if (System.currentTimeMillis() - start > 90000L) {
        throw new TimeoutException("Server did not start after 90 seconds");
      }
    }
  }

  public void stopSentryService() throws Exception {
    server.stop();
    final long start = System.currentTimeMillis();
    while (server.isRunning()) {
      Thread.sleep(1000);
      if (System.currentTimeMillis() - start > 90000L) {
        throw new TimeoutException("Server did not start after 90 seconds");
      }
    }
  }

  public void setupConf() throws Exception {
    if (kerberos) {
      setupKdc();
      kdc = getKdc();
      kdcWorkDir = getWorkDir();
      serverKeytab = new File(kdcWorkDir, "server.keytab");
      clientKeytab = new File(kdcWorkDir, "client.keytab");
      kdc.createPrincipal(serverKeytab, SERVER_PRINCIPAL);
      kdc.createPrincipal(clientKeytab, CLIENT_PRINCIPAL);
      conf.set(ServerConfig.PRINCIPAL, getServerKerberosName());
      conf.set(ServerConfig.KEY_TAB, serverKeytab.getPath());
      conf.set(ServerConfig.ALLOW_CONNECT, CLIENT_KERBEROS_SHORT_NAME);
    } else {
      LOGGER.info("Stopped KDC");
      conf.set(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_NONE);
    }
    if (haEnabled) {
      zkServer = getZKServer();
      conf.set(ServerConfig.SENTRY_HA_ENABLED, "true");
      conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
      conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE, "sentry-test-case");
      if (kerberos) {
        conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_SECURITY, "true");
      }
    }
    if (pooled) {
      conf.set(ClientConfig.SENTRY_POOL_ENABLED, "true");
    }
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP);
    conf.set(ServerConfig.RPC_ADDRESS, SERVER_HOST);
    conf.set(ServerConfig.RPC_PORT, String.valueOf(0));
    dbDir = new File(Files.createTempDir(), "sentry_policy_db");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    server = new SentryServiceFactory().create(conf);
    conf.set(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress().getHostName());
    conf.set(ClientConfig.SERVER_RPC_PORT, String.valueOf(server.getAddress().getPort()));
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING,
        ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    policyFilePath = new File(dbDir, "local_policy_file.ini");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE,
        policyFilePath.getPath());
    policyFile = new PolicyFile();
  }

  public void connectToSentryService() throws Exception {
    // The client should already be logged in when running in hive/impala/solr
    // therefore we must manually login in the integration tests
    final SentryServicePolicyClientFactory clientFactory;
    if (kerberos) {
      conf.set(ServerConfig.SECURITY_USE_UGI_TRANSPORT, "false");
      clientFactory = new SentryServicePolicyClientFactory(conf);
      clientSubject = new Subject(false, Sets.newHashSet(
          new KerberosPrincipal(CLIENT_KERBEROS_NAME)), new HashSet<Object>(),
        new HashSet<Object>());
      clientLoginContext = new LoginContext("", clientSubject, null,
          KerberosConfiguration.createClientConfig(CLIENT_KERBEROS_NAME, clientKeytab));
      clientLoginContext.login();
      clientSubject = clientLoginContext.getSubject();
      client = Subject.doAs(clientSubject, new PrivilegedExceptionAction<SentryPolicyServiceClient>() {
        @Override
        public SentryPolicyServiceClient run() throws Exception {
          return clientFactory.getSentryPolicyClient();
        }
      });
    } else {
      clientFactory = new SentryServicePolicyClientFactory(conf);
      client = clientFactory.getSentryPolicyClient();
    }
  }

  @After
  public void tearDown() throws Exception {
    beforeTeardown();
    if(client != null) {
      client.close();
    }
    if (clientFactory != null) {
      clientFactory.close();
    }
    if(clientLoginContext != null) {
      try {
        clientLoginContext.logout();
      } catch (Exception e) {
        LOGGER.warn("Error logging client out", e);
      }
    }
    if(server != null) {
      server.stop();
    }
    if (dbDir != null) {
      FileUtils.deleteQuietly(dbDir);
    }
    afterTeardown();
  }

  public String getServerKerberosName() {
    return SERVER_KERBEROS_NAME;
  }

  public void beforeSetup() throws Exception {

  }
  public void afterSetup() throws Exception {

  }
  public void beforeTeardown() throws Exception {

  }
  public void afterTeardown() throws Exception {

  }
  protected static void assertOK(TSentryResponseStatus resp) {
    assertStatus(Status.OK, resp);
  }

  protected static void assertStatus(Status status, TSentryResponseStatus resp) {
    if (resp.getValue() !=  status.getCode()) {
      String message = "Expected: " + status + ", Response: " + Status.fromCode(resp.getValue())
          + ", Code: " + resp.getValue() + ", Message: " + resp.getMessage();
      String stackTrace = Strings.nullToEmpty(resp.getStack()).trim();
      if (!stackTrace.isEmpty()) {
        message += ", StackTrace: " + stackTrace;
      }
      Assert.fail(message);
    }
  }

  protected void setLocalGroupMapping(String user, Set<String> groupSet) {
    for (String group : groupSet) {
      policyFile.addGroupsToUser(user, group);
    }
  }

  protected void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
  }

  protected TestingServer getZKServer() throws Exception {
    if (!kerberos) {
      LOGGER.info("Creating a non-security ZooKeeper Server.");
      return new TestingServer();
    } else {
      LOGGER.info("Creating a security ZooKeeper Server.");
      // Not entirely sure exactly what "javax.security.auth.useSubjectCredsOnly=false" does, but it has something to do with
      // re-authenticating in cases where it otherwise wouldn't.  One of the sections on this page briefly mentions it:
      // http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

      // Setup KDC and principal
      kdc = getKdc();
      ZKKeytabFile = new File(kdcWorkDir, "test.keytab");
      kdc.createPrincipal(ZKKeytabFile, ZK_SERVER_PRINCIPAL);

      System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
      System.setProperty("zookeeper.kerberos.removeHostFromPrincipal", "true");
      System.setProperty("zookeeper.kerberos.removeRealmFromPrincipal", "true");

      JaasConfiguration.addEntry("Server", ZK_SERVER_PRINCIPAL, ZKKeytabFile.getAbsolutePath());
      // Here's where we add the "Client" to the jaas configuration, even though we'd like not to
      JaasConfiguration.addEntry("Client", SERVER_KERBEROS_NAME, serverKeytab.getAbsolutePath());
      javax.security.auth.login.Configuration.setConfiguration(JaasConfiguration.getInstance());

      System.setProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY, "Server");

      return new TestingServer();
    }

  }

  protected void runTestAsSubject(final TestOperation test) throws Exception {
    if (this.kerberos) {
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          test.runTestAsSubject();
          return null;
        }});
    } else {
      test.runTestAsSubject();
    }
  }

  protected interface TestOperation {
    public void runTestAsSubject() throws Exception;
  }

}
