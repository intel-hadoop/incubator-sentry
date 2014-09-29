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
package org.apache.sentry.provider.db.genericModel.service.thrift;

import static org.mockito.Mockito.spy;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.CommitContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

public class TestNotificationHandlerInvoker {
  private CommitContext commitContext;
  private NotificationHandler handler;
  private NotificationHandlerInvoker invoker;

  @Before
  public void setup() throws Exception {
    commitContext = new CommitContext(UUID.randomUUID(), 1L);
    handler = spy(new MockNotificationHandler());
    invoker = new NotificationHandlerInvoker(Lists.newArrayList(handler));
  }

  @Test
  public void testCreateSentryRole() throws Exception {
    TCreateSentryRoleRequest request = new TCreateSentryRoleRequest();
    TCreateSentryRoleResponse response = new TCreateSentryRoleResponse();
    invoker.create_sentry_role(commitContext, request, response);
    Mockito.verify(handler).create_sentry_role(commitContext,
        request, response);
  }

  @Test
  public void testDropSentryRole() throws Exception {
    TDropSentryRoleRequest request = new TDropSentryRoleRequest();
    TDropSentryRoleResponse response = new TDropSentryRoleResponse();
    invoker.drop_sentry_role(commitContext, request, response);
    Mockito.verify(handler).drop_sentry_role(commitContext,
        request, response);
  }

  @Test
  public void testAlterSentryRoleAddGroups() throws Exception {
    TAlterSentryRoleAddGroupsRequest request = new TAlterSentryRoleAddGroupsRequest();
    TAlterSentryRoleAddGroupsResponse response = new TAlterSentryRoleAddGroupsResponse();
    invoker.alter_sentry_role_add_groups(commitContext, request, response);
    Mockito.verify(handler).alter_sentry_role_add_groups(commitContext,
        request, response);
  }

  @Test
  public void testAlterSentryRoleDeleteGroups() throws Exception {
    TAlterSentryRoleDeleteGroupsRequest request = new TAlterSentryRoleDeleteGroupsRequest();
    TAlterSentryRoleDeleteGroupsResponse response = new TAlterSentryRoleDeleteGroupsResponse();
    invoker.alter_sentry_role_delete_groups(commitContext, request, response);
    Mockito.verify(handler).alter_sentry_role_delete_groups(commitContext,
        request, response);
  }

  @Test
  public void testAlterSentryGrantPrivilege() throws Exception {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    TAlterSentryRoleGrantPrivilegeResponse response = new TAlterSentryRoleGrantPrivilegeResponse();
    invoker.alter_sentry_role_grant_privilege(commitContext, request, response);
    Mockito.verify(handler).alter_sentry_role_grant_privilege(commitContext,
        request, response);
  }

  @Test
  public void testAlterSentryRevokePrivilege() throws Exception {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    TAlterSentryRoleRevokePrivilegeResponse response = new TAlterSentryRoleRevokePrivilegeResponse();
    invoker.alter_sentry_role_revoke_privilege(commitContext, request, response);
    Mockito.verify(handler).alter_sentry_role_revoke_privilege(commitContext,
        request, response);
  }

  @Test
  public void testDropSentryPrivilege() throws Exception {
    TDropPrivilegesRequest request = new TDropPrivilegesRequest();
    TDropPrivilegesResponse response = new TDropPrivilegesResponse();
    invoker.drop_sentry_privilege(commitContext, request, response);
    Mockito.verify(handler).drop_sentry_privilege(commitContext, request, response);
  }

  @Test
  public void testRenameSentryPrivilege() throws Exception {
    TRenamePrivilegesRequest request = new TRenamePrivilegesRequest();
    TRenamePrivilegesResponse response = new TRenamePrivilegesResponse();
    invoker.rename_sentry_privilege(commitContext, request, response);
    Mockito.verify(handler).rename_sentry_privilege(commitContext, request, response);
  }

  private static class MockNotificationHandler implements NotificationHandler {

    @Override
    public void create_sentry_role(CommitContext context,
        TCreateSentryRoleRequest request, TCreateSentryRoleResponse response) {
    }

    @Override
    public void drop_sentry_role(CommitContext context,
        TDropSentryRoleRequest request, TDropSentryRoleResponse response) {
    }

    @Override
    public void alter_sentry_role_grant_privilege(CommitContext context,
        TAlterSentryRoleGrantPrivilegeRequest request,
        TAlterSentryRoleGrantPrivilegeResponse response) {
    }

    @Override
    public void alter_sentry_role_revoke_privilege(CommitContext context,
        TAlterSentryRoleRevokePrivilegeRequest request,
        TAlterSentryRoleRevokePrivilegeResponse response) {
    }

    @Override
    public void alter_sentry_role_add_groups(CommitContext context,
        TAlterSentryRoleAddGroupsRequest request,
        TAlterSentryRoleAddGroupsResponse response) {
    }

    @Override
    public void alter_sentry_role_delete_groups(CommitContext context,
        TAlterSentryRoleDeleteGroupsRequest request,
        TAlterSentryRoleDeleteGroupsResponse response) {
    }

    @Override
    public void drop_sentry_privilege(CommitContext context,
        TDropPrivilegesRequest request, TDropPrivilegesResponse response) {
    }

    @Override
    public void rename_sentry_privilege(CommitContext context,
        TRenamePrivilegesRequest request, TRenamePrivilegesResponse response) {
    }

  }
}
