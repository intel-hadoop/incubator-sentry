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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.Field;
import org.apache.sentry.core.model.search.SearchAction;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.genericModel.service.thrift.PrivilegeObject;
import org.apache.sentry.provider.db.genericModel.service.thrift.PrivilegeObject.Builder;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * The test cases are used for search component The authorizables are COLLECTION and Field
 * The actions of search privilege are ALL,QUERY and UPDATE
 */
public class TestSearchPrivilegeSentryStore extends SentryStoreIntegrationBase {

  private static final String ADMIN_USER = "solr";
  private static final String GRANT_OPTION_USER = "user_grant_option";
  private static final String[] GRANT_OPTION_GROUP = { "group_grant_option" };
  private static final String NO_GRANT_OPTION_USER = "user_no_grant_option";
  private static final String[] NO_GRANT_OPTION_GROUP = { "group_no_grant_option" };

  private static final String SERVICE = "service";
  private static final String COLLECTION_SCOPE = "COLLECTION";
  private static final String FIELD_SCOPE = "FIELD";
  private static final String COMPONENT = "solr";
  private static final String COLLECTION_NAME = "collection1";
  private static final String NOT_COLLECTION_NAME = "not_collection1";
  private static final String FIELD_NAME = "field1";
  private static final String NOT_FIELD_NAME = "not_field1";

  @Override
  public void configure(Configuration conf) throws Exception {
    /**
     * add the solr user to admin groups
     */
    addGroupsToUser(ADMIN_USER, getAdminGroups());
    writePolicyFile();
  }

  @Override
  public SentryStoreLayer createSentryStore(Configuration conf)
      throws Exception {
    return new GMPrivilegeSentryStore(conf);
  }

  /**
   * Grant query privilege to role r1 and there is no privilege related this
   * collection existed
   */
  @Test
  public void testGrantPrivilege() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .build();

    sentryStore.createSentryRole(roleName, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, queryPrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
  }

  /**
   * Grant query privilege to role r1 and there is ALL privilege related this
   * collection existed
   */
  @Test
  public void testGrantPrivilegeWithAllPrivilegeExist() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject allPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.ALL.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .build();

    sentryStore.createSentryRole(roleName, grantor);
    /**
     * grant all privilege to role r1
     */
    sentryStore.alterSentryRoleGrantPrivilege(roleName, allPrivilege, grantor);
    /**
     * check role r1 truly has the privilege been granted
     */
    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));

    PrivilegeObject queryPrivilege = new Builder(allPrivilege)
        .setAction(SearchAction.QUERY.getValue())
        .build();

    /**
     * grant query privilege to role r1
     */
    sentryStore.alterSentryRoleGrantPrivilege(roleName, queryPrivilege, grantor);
    /**
     * all privilege has been existed, the query privilege will not persistent
     */
    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
  }

  /**
   * Grant query privilege to role r1 and there are query and update privileges
   * related this collection existed
   */
  @Test
  public void testGrantALLPrivilegeWithOtherPrivilegesExist() throws Exception {
    String roleName1 = "r1";
    String roleName2 = "r2";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;

    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .build();

    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
        .setAction(SearchAction.UPDATE.getValue())
        .build();

    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.createSentryRole(roleName2, grantor);
    /**
     * grant query and update privilege to role r1 and role r2
     */
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, queryPrivilege, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, updatePrivilege,grantor);
    assertEquals(Sets.newHashSet(queryPrivilege, updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    sentryStore.alterSentryRoleGrantPrivilege(roleName2, queryPrivilege, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, updatePrivilege,grantor);
    assertEquals(Sets.newHashSet(queryPrivilege, updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));

    PrivilegeObject allPrivilege = new Builder(queryPrivilege)
        .setAction(SearchAction.ALL.getValue())
        .build();

    /**
     * grant all privilege to role r1
     */
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, allPrivilege, grantor);

    /**
     * check the query and update privileges of roleName1 will be removed because of ALl privilege
     * granted
     */
    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    /**
     * check the query and update privileges of roleName2 will not affected and exist
     */
    assertEquals(Sets.newHashSet(queryPrivilege, updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));
  }

  @Test
  public void testGrantRevokeCheckWithGrantOption() throws Exception {

    addGroupsToUser(GRANT_OPTION_USER, GRANT_OPTION_GROUP);
    addGroupsToUser(NO_GRANT_OPTION_USER, NO_GRANT_OPTION_GROUP);
    writePolicyFile();

    String roleName1 = "r1";
    String roleName2 = "r2";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.createSentryRole(roleName2, grantor);
    /**
     * grant query privilege to role r1 with grant option
     */
    PrivilegeObject queryPrivilege1 = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .withGrantOption(true)
        .build();

    sentryStore.alterSentryRoleGrantPrivilege(roleName1, queryPrivilege1,
        ADMIN_USER);
    assertEquals(Sets.newHashSet(queryPrivilege1),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));
    /**
     * grant query privilege to role r2 no grant option
     */
    PrivilegeObject queryPrivilege2 = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .withGrantOption(false).build();

    sentryStore.alterSentryRoleGrantPrivilege(roleName2, queryPrivilege2,
        ADMIN_USER);
    assertEquals(Sets.newHashSet(queryPrivilege2),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));

    sentryStore.alterSentryRoleAddGroups(roleName1,
        Sets.newHashSet(GRANT_OPTION_GROUP), grantor);
    sentryStore.alterSentryRoleAddGroups(roleName2,
        Sets.newHashSet(NO_GRANT_OPTION_GROUP), grantor);

    String roleName3 = "r3";
    sentryStore.createSentryRole(roleName3, grantor);
    /**
     * the user with grant option grant query privilege to rolr r3
     */
    try{
      sentryStore.alterSentryRoleGrantPrivilege(roleName3, queryPrivilege1,
          GRANT_OPTION_USER);
    } catch (SentryGrantDeniedException e) {
      fail("SentryGrantDeniedException shouldn't have been thrown");
    }

    /**
     * the user with grant option revoke query privilege to rolr r3
     */
    try{
      sentryStore.alterSentryRoleRevokePrivilege(roleName3, queryPrivilege1,
          GRANT_OPTION_USER);
    } catch (SentryGrantDeniedException e) {
      fail("SentryGrantDeniedException shouldn't have been thrown");
    }

    /**
     * the user with no grant option grant query privilege to rolr r3, it will
     * throw SentryGrantDeniedException
     */
    try {
      sentryStore.alterSentryRoleGrantPrivilege(roleName3, queryPrivilege2,
          NO_GRANT_OPTION_USER);
      fail("SentryGrantDeniedException should have been thrown");
    } catch (SentryGrantDeniedException e) {
      //ignore the exception
    }

    /**
     * the user with no grant option revoke query privilege to rolr r3, it will
     * throw SentryGrantDeniedException
     */
    try {
      sentryStore.alterSentryRoleGrantPrivilege(roleName3, queryPrivilege2,
          NO_GRANT_OPTION_USER);
      fail("SentryGrantDeniedException should have been thrown");
    } catch (SentryGrantDeniedException e) {
      //ignore the exception
    }
  }

  @Test
  public void testGrantWithGrantOption() throws Exception {

    addGroupsToUser(GRANT_OPTION_USER, GRANT_OPTION_GROUP);
    addGroupsToUser(NO_GRANT_OPTION_USER, NO_GRANT_OPTION_GROUP);
    writePolicyFile();

    String roleName1 = "r1";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1, grantor);
    /**
     * grant query privilege to role r1 with grant option
     */
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .withGrantOption(true)
        .build();

    sentryStore.alterSentryRoleGrantPrivilege(roleName1, queryPrivilege,ADMIN_USER);
    sentryStore.alterSentryRoleAddGroups(roleName1,
        Sets.newHashSet(GRANT_OPTION_GROUP), grantor);

    /**
     * the user with grant option grant query privilege to rolr r2
     */
    String roleName2 = "r2";
    sentryStore.createSentryRole(roleName2, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, queryPrivilege, GRANT_OPTION_USER);

    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));

  }


  /**
   * Grant query and update privileges to role r1 and revoke query privilege
   * there is left update privilege related to role r1
   */
  @Test
  public void testRevokePrivilege() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .setScope(FIELD_SCOPE)
        .build();

    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
        .setAction(SearchAction.UPDATE.getValue())
        .build();

    sentryStore.createSentryRole(roleName, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, queryPrivilege, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, updatePrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
    /**
     * revoke query privilege
     */
    sentryStore.alterSentryRoleRevokePrivilege(roleName, queryPrivilege, grantor);
    assertEquals(Sets.newHashSet(updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
  }

  /**
   * Grant query and update privileges to role r1 and revoke all privilege,
   * there is no privilege related to role r1
   */
  @Test
  public void testRevokeAllPrivilege() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME),new Field(FIELD_NAME)))
        .setScope(FIELD_SCOPE)
        .build();

    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
        .setAction(SearchAction.UPDATE.getValue())
        .build();

    sentryStore.createSentryRole(roleName, grantor);

    sentryStore.alterSentryRoleGrantPrivilege(roleName, queryPrivilege, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, updatePrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
    /**
     * revoke all privilege
     */
    PrivilegeObject allPrivilege = new Builder(queryPrivilege)
        .setAction(SearchAction.ALL.getValue())
        .build();

    sentryStore.alterSentryRoleRevokePrivilege(roleName, allPrivilege, grantor);

    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
  }

  /**
   * Grant all privilege to role r1 and revoke query privilege
   * there is update privilege related to role r1
   */
  @Test
  public void testRevokePrivilegeWithAllPrivilegeExist() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject allPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .setScope(FIELD_SCOPE)
        .build();

    sentryStore.createSentryRole(roleName, grantor);

    sentryStore.alterSentryRoleGrantPrivilege(roleName, allPrivilege, grantor);

    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
    /**
     * revoke update privilege
     */
    PrivilegeObject updatePrivilege = new Builder(allPrivilege)
        .setAction(SearchAction.UPDATE.getValue())
        .build();

    PrivilegeObject queryPrivilege = new Builder(allPrivilege)
        .setAction(SearchAction.QUERY.getValue())
        .build();

    sentryStore.alterSentryRoleRevokePrivilege(roleName, updatePrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
  }

  @Test
  public void testRevokeParentPrivilegeWithChildsExist() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject updatePrivilege1 = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.UPDATE.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .setScope(FIELD_SCOPE)
        .build();

    PrivilegeObject queryPrivilege1 = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME),new Field(FIELD_NAME)))
        .setScope(FIELD_SCOPE)
        .build();

    PrivilegeObject queryPrivilege2 = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(NOT_COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .build();

    sentryStore.createSentryRole(roleName, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, updatePrivilege1, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName, queryPrivilege1, grantor);

    sentryStore.alterSentryRoleGrantPrivilege(roleName, queryPrivilege2, grantor);

    /**
     * revoke all privilege with collection[COLLECTION_NAME=collection1] and its child privileges
     */
    PrivilegeObject allPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.ALL.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .build();

    sentryStore.alterSentryRoleRevokePrivilege(roleName, allPrivilege, grantor);
    assertEquals(Sets.newHashSet(queryPrivilege2),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName)));
  }

  @Test
  public void testRevokeWithGrantOption() throws Exception {

    addGroupsToUser(GRANT_OPTION_USER, GRANT_OPTION_GROUP);
    addGroupsToUser(NO_GRANT_OPTION_USER, NO_GRANT_OPTION_GROUP);
    writePolicyFile();

    String roleName1 = "r1";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1, grantor);
    /**
     * grant query privilege to role r1 with grant option
     */
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .withGrantOption(true)
        .build();

    sentryStore.alterSentryRoleGrantPrivilege(roleName1, queryPrivilege,
        ADMIN_USER);
    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    sentryStore.alterSentryRoleAddGroups(roleName1,
        Sets.newHashSet(GRANT_OPTION_GROUP), grantor);

    String roleName2 = "r2";
    sentryStore.createSentryRole(roleName2, grantor);
    /**
     * the user with grant option grant query privilege to rolr r2
     */
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, queryPrivilege,
        GRANT_OPTION_USER);
    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));

    /**
     * the user with grant option revoke query privilege to rolr r3
     */
    sentryStore.alterSentryRoleRevokePrivilege(roleName2, queryPrivilege, GRANT_OPTION_USER);
    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));
  }

  @Test
  public void testDropPrivilege() throws Exception{
    String roleName1 = "r1";
    String roleName2 = "r2";
    String grantor = ADMIN_USER;

    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .setScope(FIELD_SCOPE)
        .build();

    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
        .setAction(SearchAction.UPDATE.getValue())
        .build();

    /**
     * grant query and update privilege to role r1 and r2
     */
    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, queryPrivilege, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, updatePrivilege, grantor);

    sentryStore.createSentryRole(roleName2, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, queryPrivilege, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, updatePrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));
    /**
     * drop query privilege
     */
    sentryStore.dropSentryPrivilege(queryPrivilege, grantor);

    assertEquals(Sets.newHashSet(updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));

    /**
     * drop ALL privilege
     */
    PrivilegeObject allPrivilege = new Builder(queryPrivilege)
        .setAction(SearchAction.ALL.getValue())
        .build();

    sentryStore.dropSentryPrivilege(allPrivilege, grantor);

    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));

    /**
     * grant query and update field scope[collection1,field1] privilege to role r1
     * drop collection scope[collection1] privilege
     * there is no privilege
     */
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, queryPrivilege, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, updatePrivilege, grantor);

    PrivilegeObject parentPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.ALL.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .build();

    sentryStore.dropSentryPrivilege(parentPrivilege, grantor);
    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));
  }

  @Test
  public void testRenamePrivilege() throws Exception{
    String roleName1 = "r1";
    String roleName2 = "r2";
    String grantor = ADMIN_USER;

    List<? extends Authorizable> oldAuthoriables = Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME));
    List<? extends Authorizable> newAuthoriables = Arrays.asList(new Collection(COLLECTION_NAME), new Field(NOT_FIELD_NAME));

    PrivilegeObject oldQueryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(oldAuthoriables)
        .setScope(FIELD_SCOPE)
        .build();

    PrivilegeObject oldUpdatePrivilege = new Builder(oldQueryPrivilege)
        .setAction(SearchAction.UPDATE.getValue())
        .build();

    PrivilegeObject oldALLPrivilege = new Builder(oldQueryPrivilege)
        .setAction(SearchAction.ALL.getValue())
        .build();


    PrivilegeObject newQueryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(newAuthoriables)
        .setScope(FIELD_SCOPE)
        .build();

    PrivilegeObject newUpdatePrivilege = new Builder(newQueryPrivilege)
        .setAction(SearchAction.UPDATE.getValue())
        .build();

    PrivilegeObject newALLPrivilege = new Builder(newQueryPrivilege)
        .setAction(SearchAction.ALL.getValue())
        .build();


    /**
     * grant query and update privilege to role r1
     * grant all privilege to role r2
     */
    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, oldQueryPrivilege, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, oldUpdatePrivilege, grantor);

    sentryStore.createSentryRole(roleName2, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, oldALLPrivilege, grantor);

    assertEquals(Sets.newHashSet(oldQueryPrivilege,oldUpdatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(oldALLPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));
    /**
     * rename old query privilege to new query privilege
     */
    sentryStore.renameSentryPrivilege(COMPONENT, SERVICE,
                                      oldAuthoriables,
                                      newAuthoriables,
                                      grantor);

    assertEquals(Sets.newHashSet(newQueryPrivilege,newUpdatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(newALLPrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));
    /**
     * rename collection scope[collection=collection1] privilege to [collection=not_collection1]
     * These privileges belong to collection scope[collection=collection1] will change to
     * [collection=not_collection1]
     */

    List<? extends Authorizable> newAuthoriables1 = Arrays.asList(new Collection(NOT_COLLECTION_NAME),new Field(NOT_FIELD_NAME));

    PrivilegeObject newQueryPrivilege1 = new Builder(newQueryPrivilege)
          .setAuthorizables(newAuthoriables1)
          .build();

    PrivilegeObject newUpdatePrivilege1 = new Builder(newUpdatePrivilege)
          .setAuthorizables(newAuthoriables1)
          .build();

    PrivilegeObject newALLPrivilege1 = new Builder(newALLPrivilege)
          .setAuthorizables(newAuthoriables1)
          .build();

    sentryStore.renameSentryPrivilege(COMPONENT, SERVICE,
        Arrays.asList(new Collection(COLLECTION_NAME)),
        Arrays.asList(new Collection(NOT_COLLECTION_NAME)),
        grantor);

    assertEquals(Sets.newHashSet(newQueryPrivilege1,newUpdatePrivilege1),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(newALLPrivilege1),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName2)));
  }

  @Test
  public void testGetPrivilegesByRoleName() throws Exception {
    String roleName1 = "r1";
    String roleName2 = "r2";
    String grantor = "g1";

    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .build();

    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, queryPrivilege,
        ADMIN_USER);

    PrivilegeObject updatePrivilege = new Builder()
        .setComponent(COMPONENT)
        .setAction(SearchAction.QUERY.getValue())
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .setScope(COLLECTION_SCOPE)
        .build();

    sentryStore.createSentryRole(roleName2, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, updatePrivilege,
        ADMIN_USER);

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRoleName(Sets.newHashSet(roleName1,roleName2)));

  }

  @Test
  public void testGetPrivilegesByProvider() throws Exception {
    String roleName1 = "r1";
    String roleName2 = "r2";
    String roleName3 = "r3";
    String group = "g3";
    String grantor = ADMIN_USER;

    String service1 = "service1";

    PrivilegeObject queryPrivilege1 = new Builder()
         .setComponent(COMPONENT)
         .setAction(SearchAction.QUERY.getValue())
         .setService(service1)
         .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
         .setScope(COLLECTION_SCOPE)
         .build();

    PrivilegeObject updatePrivilege1 = new Builder()
         .setComponent(COMPONENT)
         .setAction(SearchAction.UPDATE.getValue())
         .setService(service1)
         .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
         .setScope(FIELD_SCOPE)
         .build();

    PrivilegeObject queryPrivilege2 = new Builder()
         .setComponent(COMPONENT)
         .setAction(SearchAction.QUERY.getValue())
         .setService(service1)
         .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
         .setScope(COLLECTION_SCOPE)
         .build();

    PrivilegeObject updatePrivilege2 = new Builder()
         .setComponent(COMPONENT)
         .setAction(SearchAction.UPDATE.getValue())
         .setService(service1)
         .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
         .setScope(FIELD_SCOPE)
         .build();

    sentryStore.createSentryRole(roleName1, grantor);
    sentryStore.createSentryRole(roleName2, grantor);
    sentryStore.createSentryRole(roleName3, grantor);

    sentryStore.alterSentryRoleAddGroups(roleName3, Sets.newHashSet(group), grantor);

    sentryStore.alterSentryRoleGrantPrivilege(roleName1, queryPrivilege1, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName1, updatePrivilege1, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName2, queryPrivilege2, grantor);
    sentryStore.alterSentryRoleGrantPrivilege(roleName3, updatePrivilege2, grantor);

    assertEquals(Sets.newHashSet(updatePrivilege1, queryPrivilege1, queryPrivilege2, updatePrivilege2),
        sentryStore.getPrivilegesByProvider(COMPONENT, service1, null, null, null));

    assertEquals(Sets.newHashSet(updatePrivilege1, queryPrivilege1),
        sentryStore.getPrivilegesByProvider(COMPONENT, service1, Sets.newHashSet(roleName1), null, null));

    assertEquals(Sets.newHashSet(updatePrivilege1, queryPrivilege1, queryPrivilege2),
        sentryStore.getPrivilegesByProvider(COMPONENT, service1, Sets.newHashSet(roleName1,roleName2),
            null, null));

    assertEquals(Sets.newHashSet(updatePrivilege1, queryPrivilege1, queryPrivilege2, updatePrivilege2),
        sentryStore.getPrivilegesByProvider(COMPONENT, service1, Sets.newHashSet(roleName1,roleName2),
            Sets.newHashSet(group), null));

    List<? extends Authorizable> authorizables = Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME));
    assertEquals(Sets.newHashSet(updatePrivilege1, updatePrivilege2),
        sentryStore.getPrivilegesByProvider(COMPONENT, service1, Sets.newHashSet(roleName1,roleName2),
            Sets.newHashSet(group), authorizables));
  }
}
