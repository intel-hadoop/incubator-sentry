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

import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.junit.Test;
import com.google.common.collect.Sets;

public class TestRoleAndGroupSentryStore extends SentryStoreIntegrationBase{

  @Override
  public SentryStoreLayer createSentryStore(Configuration conf) throws Exception {
    return new GMPrivilegeSentryStore(conf);
  }

  @Test
  public void testCreateDropRole() throws Exception {
    String roleName = "test-drop-role";
    String grantor = "grantor";
    long seqId = sentryStore.createSentryRole(roleName, grantor).getSequenceId();
    assertEquals(seqId + 1, sentryStore.dropSentryRole(roleName, grantor).getSequenceId());
  }

  @Test
  public void testCaseInsensitiveCreateDropRole() throws Exception {
    String roleName1 = "test";
    String roleName2 = "TeSt";
    String grantor = "grantor";
    sentryStore.createSentryRole(roleName1, grantor);
    boolean alreadyExist = false;
    try {
      sentryStore.createSentryRole(roleName2, grantor);
    } catch (SentryAlreadyExistsException e) {
      alreadyExist = true;
    }
    assertTrue(alreadyExist);
    boolean noExist = false;
    try {
      sentryStore.dropSentryRole(roleName2, grantor);
    } catch (SentryNoSuchObjectException e) {
      noExist = true;
    }
    assertTrue(!noExist);
  }

  @Test(expected=SentryAlreadyExistsException.class)
  public void testCreateDuplicateRole() throws Exception {
    String roleName = "test-dup-role";
    String grantor = "grantor";
    sentryStore.createSentryRole(roleName, grantor);
    sentryStore.createSentryRole(roleName, grantor);
  }

  @Test(expected=SentryNoSuchObjectException.class)
  public void testDropNotExistRole() throws Exception {
    String roleName = "not-exist";
    String grantor = "grantor";
    sentryStore.dropSentryRole(roleName, grantor);
  }

  @Test(expected = SentryNoSuchObjectException.class)
  public void testAddGroupsNonExistantRole()
      throws Exception {
    String roleName = "non-existant-role";
    String grantor = "grantor";
    sentryStore.alterSentryRoleAddGroups(roleName, Sets.newHashSet("g1"), grantor);
  }

  @Test(expected = SentryNoSuchObjectException.class)
  public void testDeleteGroupsNonExistantRole()
      throws Exception {
    String roleName = "non-existant-role";
    String grantor = "grantor";
    sentryStore.alterSentryRoleDeleteGroups(roleName, Sets.newHashSet("g1"), grantor);
  }

  @Test
  public void testAddDeleteRoleToGroups() throws Exception {
    String role1 = "r1", role2 = "r2";
    Set<String> twoGroups = Sets.newHashSet("g1", "g2");
    Set<String> oneGroup = Sets.newHashSet("g3");
    String grantor = "grantor";

    sentryStore.createSentryRole(role1, grantor);
    sentryStore.createSentryRole(role2, grantor);

    sentryStore.alterSentryRoleAddGroups(role1, twoGroups, grantor);
    assertEquals(twoGroups, sentryStore.getGroupsByRoleNames(Sets.newHashSet(role1)));

    assertEquals(Sets.newHashSet(role1), sentryStore.getRolesByGroupNames(twoGroups));

    sentryStore.alterSentryRoleAddGroups(role2, oneGroup, grantor);
    assertEquals(oneGroup, sentryStore.getGroupsByRoleNames(Sets.newHashSet(role2)));

    sentryStore.alterSentryRoleDeleteGroups(role1, Sets.newHashSet("g1"), grantor);
    assertEquals(Sets.newHashSet("g2"), sentryStore.getGroupsByRoleNames(Sets.newHashSet(role1)));

    sentryStore.alterSentryRoleDeleteGroups(role2, oneGroup, grantor);
    assertEquals(Sets.newHashSet(), sentryStore.getGroupsByRoleNames(Sets.newHashSet(role2)));
  }

  @Test
  public void testGetRolesByGroupNames() throws Exception {
    String role1 = "r1", role2 = "r2";
    Set<String> twoGroups = Sets.newHashSet("g1", "g2");
    String grantor = "grantor";

    sentryStore.createSentryRole(role1, grantor);
    sentryStore.createSentryRole(role2, grantor);

    sentryStore.alterSentryRoleAddGroups(role1, twoGroups, grantor);
    sentryStore.alterSentryRoleAddGroups(role2, twoGroups, grantor);

    assertEquals(Sets.newHashSet(role1,role2), sentryStore.getRolesByGroupNames(twoGroups));
  }

  @Test
  public void testGetGroupsByRoleNames() throws Exception {
    String role1 = "r1", role2 = "r2";
    Set<String> twoGroups = Sets.newHashSet("g1", "g2");
    String grantor = "grantor";

    sentryStore.createSentryRole(role1, grantor);
    sentryStore.createSentryRole(role2, grantor);

    sentryStore.alterSentryRoleAddGroups(role1, twoGroups, grantor);
    sentryStore.alterSentryRoleAddGroups(role2, twoGroups, grantor);

    assertEquals(twoGroups, sentryStore.getGroupsByRoleNames(Sets.newHashSet(role1)));
    assertEquals(twoGroups, sentryStore.getGroupsByRoleNames(Sets.newHashSet(role2)));
    assertEquals(twoGroups, sentryStore.getGroupsByRoleNames(Sets.newHashSet(role1,role2)));
  }
}
