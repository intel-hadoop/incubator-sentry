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

import java.util.Arrays;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.Field;
import org.apache.sentry.core.model.search.SearchAction;
import org.apache.sentry.provider.db.genericModel.service.thrift.PrivilegeObject.Builder;

import org.junit.Test;


import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class TestPrivilegeUtils {

  private static final String SERVICE = "service";
  //Test for Search Component
  private static final String COLLECTION_SCOPE = "COLLECTION";
  private static final String FIELD_SCOPE = "FIELD";
  private static final String SEARCH_COMPONENT = "solr";
  private static final String COLLECTION_NAME = "collection1";
  private static final String FIELD_NAME = "field1";
  private static final String NOT_FIELD_NAME = "not_field1";
  /**
   * Test implies with search privilege
   * @throws Exception
   */
  @Test
  public void testSearchImplies() throws Exception{
    testSearchImpliesEqualAuthorizable();
    testSearchImpliesDifferentAuthorizable();
    testSearchImpliesAction();
  }

  /**
   * The requested privilege has the different authorizable size with the persistent privilege
   * @throws Exception
   */
  @Test
  public void testSearchImpliesDifferentAuthorizable() throws Exception {
    /**
     * Test the scope of persistent privilege is the larger than the requested privilege
     */
    PrivilegeObject existPrivilege1 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables( Arrays.asList(new Collection(COLLECTION_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(COLLECTION_SCOPE)
    .build();

    PrivilegeObject requestPrivilege1 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setScope(FIELD_SCOPE)
    .setService(SERVICE)
    .build();

    assertTrue(PrivilegeUtil.implies(existPrivilege1, requestPrivilege1));

    /**
     * Test the persistent privilege has the same scope as request privilege
     */
    PrivilegeObject existPrivilege2 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables( Arrays.asList(new Collection(COLLECTION_NAME)) )
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(COLLECTION_SCOPE)
    .build();

    PrivilegeObject requestPrivilege2 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setScope(COLLECTION_SCOPE)
    .setService(SERVICE)
    .build();
    assertTrue(PrivilegeUtil.implies(existPrivilege2, requestPrivilege2));

    /**
     * Test the persistent privilege is less than  the request privilege
     */
    PrivilegeObject existPrivilege3 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables( Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();
    assertFalse(PrivilegeUtil.implies(existPrivilege3, requestPrivilege2));

    /**
     * Test the persistent privilege is less than  the request privilege,
     * but the name of left authorizable is ALL
     */
    PrivilegeObject existPrivilege4 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables( Arrays.asList(new Collection(COLLECTION_NAME), new Field(AccessConstants.ALL)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(COLLECTION_SCOPE)
    .build();
    assertTrue(PrivilegeUtil.implies(existPrivilege4, requestPrivilege2));
  }

  /**
   * The requested privilege has the same authorizable as with the persistent privilege,
   * but the theirs actions are different
   */
  @Test
  public void testSearchImpliesAction() throws Exception {
    /**
     * action is equal
     */
    PrivilegeObject existPrivilege1 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables( Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();

    PrivilegeObject requestPrivilege1 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setScope(FIELD_SCOPE)
    .setService(SERVICE)
    .build();

    assertTrue(PrivilegeUtil.implies(existPrivilege1, requestPrivilege1));

    /**
     * action isn't equal
     */
    PrivilegeObject existPrivilege2 = new Builder()
    .setAction(SearchAction.UPDATE.getValue())
    .setAuthorizables( Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();

    assertFalse(PrivilegeUtil.implies(existPrivilege2, requestPrivilege1));

    /**
     * action isn't equal,but the requested privilege has the ALL action
     */
    PrivilegeObject requestPrivilege2 = new Builder()
    .setAction(SearchAction.ALL.getValue())
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();
    assertFalse(PrivilegeUtil.implies(existPrivilege1, requestPrivilege2));
    /**
     * action isn't equal,but the persistent privilege has the ALL action
     */
    PrivilegeObject existPrivilege3 = new Builder()
    .setAction(SearchAction.ALL.getValue())
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();
    assertTrue(PrivilegeUtil.implies(existPrivilege3, requestPrivilege1));
  }

  /**
   * The requested privilege has the same authorizable as with the persistent privilege
   * @throws Exception
   */
  @Test
  public void testSearchImpliesEqualAuthorizable() throws Exception {
    /**
     * The authorizables are equal
     */
    PrivilegeObject existPrivilege1 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables( Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();

    PrivilegeObject requestPrivilege1 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setScope(FIELD_SCOPE)
    .setService(SERVICE)
    .build();

    assertTrue(PrivilegeUtil.implies(existPrivilege1, requestPrivilege1));

    /**
     * The authorizables aren't equal
     */
    PrivilegeObject requestPrivilege2 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(NOT_FIELD_NAME)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();
    assertFalse(PrivilegeUtil.implies(existPrivilege1, requestPrivilege2));

    /**
     * The authorizables aren't equal,but the persistent privilege has the ALL name
     */
    PrivilegeObject existPrivilege2 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables( Arrays.asList(new Collection(COLLECTION_NAME), new Field(AccessConstants.ALL)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();
    assertTrue(PrivilegeUtil.implies(existPrivilege2, requestPrivilege2));

    /**
     * The authorizables aren't equal,but the request privilege has the ALL name
     */
    PrivilegeObject requestPrivilege3 = new Builder()
    .setAction(SearchAction.QUERY.getValue())
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(AccessConstants.ALL)))
    .setComponent(SEARCH_COMPONENT)
    .setService(SERVICE)
    .setScope(FIELD_SCOPE)
    .build();
    assertTrue(PrivilegeUtil.implies(existPrivilege1, requestPrivilege3));
  }
}
