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
package org.apache.sentry.provider.db.genericModel.service.persistent;

import java.util.List;
import java.util.Set;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.genericModel.service.thrift.PrivilegeObject;
import org.apache.sentry.provider.db.service.persistent.CommitContext;

/**
 * Sentry store for persistent the authorize object to database
 */
public interface SentryStoreLayer {
  /**
   * Create a sentry role and persist it.
   *
   * @param roleName
   *          : Name of the role being persisted
   * @param requestorUserName
   *          : user on whose behalf the request is issued
   * @returns commit context used for notification handlers
   * @throws SentryAlreadyExistsException
   */
  public CommitContext createSentryRole(String roleName,
      String requestorUserName) throws SentryAlreadyExistsException;

  /**
   * drop a sentry role
   *
   * @param roleName
   *          : Name of the role being dropped
   * @param requestorUserName
   *          : user on whose behalf the request is issued
   * @returns commit context used for notification handlers
   * @throws SentryNoSuchObjectException
   */
  public CommitContext dropSentryRole(String roleName,
      String requestorUserName) throws SentryNoSuchObjectException;

  /**
   * add a sentry role to specific groups.
   *
   * @param roleName
   *          : Name of the role will be added
   * @param groupNames
   *          : The specific name of groups
   * @param requestorUserName
   *          : user on whose behalf the request is issued
   * @returns commit context used for notification handlers
   * @throws SentryNoSuchObjectException
   */
  public CommitContext alterSentryRoleAddGroups(String roleName,
      Set<String> groupNames, String requestorUserName)
      throws SentryNoSuchObjectException;

  /**
   * delete a sentry role from specific groups.
   *
   * @param roleName
   *          : Name of the role will be deleted
   * @param groupNames
   *          : The specific name of groups
   * @param requestorUserName
   *          : user on whose behalf the request is issued
   * @returns commit context used for notification handlers
   * @throws SentryNoSuchObjectException
   */
  public CommitContext alterSentryRoleDeleteGroups(String roleName,
      Set<String> groupNames, String requestorUserName)
      throws SentryNoSuchObjectException;

  /**
   * grant a sentry privilege to specific role.
   *
   * @param roleName
   *          : the specific role will get the privilege
   * @param privilege
   *          : the privilege object will be granted
   * @param grantorPrincipal
   *          : user on whose behalf the request is issued
   * @param requestorGroups
   *          : requestorUserName belongs to groups
   * @returns commit context used for notification handlers
   * @throws SentryUserException
   */
  public CommitContext alterSentryRoleGrantPrivilege(String roleName,
      PrivilegeObject privilege, String grantorPrincipal)
      throws SentryUserException;

  /**
   * revoke a sentry privilege to specific role.
   *
   * @param roleName
   *          : the specific role will remove the privilege
   * @param privilege
   *          : the privilege object will revoked
   * @param grantorPrincipal
   *          : user on whose behalf the request is issued
   * @param requestorGroups
   *          : requestorUserName belongs to groups
   * @returns commit context used for notification handlers
   * @throws SentryUserException
   */
  public CommitContext alterSentryRoleRevokePrivilege(String roleName,
      PrivilegeObject privilege, String grantorPrincipal)
      throws SentryUserException;

  /**
   * rename sentry privilege
   *
   * @param the name of component
   * @param the name of service
   * @param oldAuthorizables
   * @param newAuthorizables
   * @param requestorUserName
   *          : user on whose behalf the request is issued
   * @returns commit context used for notification handlers
   * @throws SentryUserException
   */
  public CommitContext renameSentryPrivilege(
      String component, String service, List<? extends Authorizable> oldAuthorizables,
      List<? extends Authorizable> newAuthorizables, String requestorUserName)
          throws SentryUserException;

  /**
   * drop sentry privilege
   *
   * @param privilege
   *          : the privilege will be dropped
   * @param requestorUserName
   *          : user on whose behalf the request is issued
   * @param groups
   *          : user belongs to specific groups
   * @returns commit context used for notification handlers
   * @throws SentryUserException
   */
  public CommitContext dropSentryPrivilege(PrivilegeObject privilege,
      String requestorUserName) throws SentryUserException;

  /**
   * get roleNames.
   * @param groupNames
   * @returns the set of roleNames
   */
  public Set<String> getRolesByGroupNames(Set<String> groupNames);

  /**
   * get groupNames.
   * @param roleNames
   * @returns the set of groupNames
   */
  public Set<String> getGroupsByRoleNames(Set<String> roleNames);

  /**
   * get sentry privileges
   * @param roleNames
   * @returns the set of privileges
   */
  public Set<PrivilegeObject> getPrivilegesByRoleName(Set<String> roleNames);

  /**
   * get sentry privileges from provider as followings:
   * @param: the name of component
   * @param: the name of service
   * @param: the roleNames
   * @param: the groupNames
   * @param: the authorizables
   * @returns the set of privileges
   * @throws SentryUserException
   */
  public Set<PrivilegeObject> getPrivilegesByProvider(String component, String service,Set<String> roleNames,
       Set<String> groupNames, List<? extends Authorizable> authorizables)
       throws SentryUserException;
  /**
   * close sentryStore
   */
  public void close();

}
