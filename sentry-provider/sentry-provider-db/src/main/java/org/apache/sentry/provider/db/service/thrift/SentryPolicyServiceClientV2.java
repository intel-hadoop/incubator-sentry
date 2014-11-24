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

package org.apache.sentry.provider.db.service.thrift;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.model.db.PrivilegeInfo;
import org.apache.sentry.service.thrift.ServiceConstants.ThriftConstants;
import org.apache.sentry.service.thrift.Status;
import org.apache.thrift.TException;

public class SentryPolicyServiceClientV2 extends SentryPolicyServiceClient {

  private static final String THRIFT_EXCEPTION_MESSAGE = "Thrift exception occured ";

  public SentryPolicyServiceClientV2(Configuration conf) throws IOException {
    super(conf);
  }

  /**
   * Grant privilege to SentryStore via thrift API
   *
   * @param requestorUserName
   * @param roleName
   * @param privInfo
   * @throws SentryUserException
   */
  public void grantPrivilege(String requestorUserName, String roleName, PrivilegeInfo privInfo)
      throws SentryUserException {
    TAlterSentryRoleGrantPrivilegeRequest request = new TAlterSentryRoleGrantPrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    TSentryPrivilege privilege = convert2TSentryPrivilege(privInfo);
    request.setPrivilege(privilege);
    try {
      TAlterSentryRoleGrantPrivilegeResponse response = client.alter_sentry_role_grant_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  /**
   * Revoke privilege from SentryStore via thrift API
   *
   * @param requestorUserName
   * @param roleName
   * @param privInfo
   * @throws SentryUserException
   */
  public void revokePrivilege(String requestorUserName, String roleName, PrivilegeInfo privInfo)
      throws SentryUserException {
    TAlterSentryRoleRevokePrivilegeRequest request = new TAlterSentryRoleRevokePrivilegeRequest();
    request.setProtocol_version(ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
    request.setRequestorUserName(requestorUserName);
    request.setRoleName(roleName);
    TSentryPrivilege privilege = convert2TSentryPrivilege(privInfo);
    request.setPrivilege(privilege);
    try {
      TAlterSentryRoleRevokePrivilegeResponse response = client.alter_sentry_role_revoke_privilege(request);
      Status.throwIfNotOk(response.getStatus());
    } catch (TException e) {
      throw new SentryUserException(THRIFT_EXCEPTION_MESSAGE, e);
    }
  }

  private TSentryPrivilege convert2TSentryPrivilege(PrivilegeInfo privInfo) {
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(privInfo.getPrivilegeScope());
    privilege.setServerName(privInfo.getServerName());
    privilege.setURI(privInfo.getURI());
    privilege.setDbName(privInfo.getDbName());
    privilege.setTableName(privInfo.getTableOrViewName());
    privilege.setAction(privInfo.getAction());
    privilege.setCreateTime(System.currentTimeMillis());
    privilege.setGrantOption(convertTSentryGrantOption(privInfo.getGrantOption()));
    return privilege;
  }
}
