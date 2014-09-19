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
import static junit.framework.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.Field;
import org.apache.sentry.core.model.search.SearchModelAuthorizable;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestHierarchyStore {
  @Test
  public void testSearchHierarchy() throws Exception {
    String component = "SOLR";
    Integer tag = 1;
    String[] hierarchys = new String[]{
          SearchModelAuthorizable.AuthorizableType.Collection.name(),
          SearchModelAuthorizable.AuthorizableType.Field.name()};

    if (!Arrays.equals(hierarchys,  HierarchyStore.getHierarchy(component, tag))) {
      fail("get hierarchy failed - expected " + Arrays.toString(hierarchys)
            + " got " + Arrays.toString(HierarchyStore.getHierarchy(component, tag)) );
    }

    List<Authorizable> authorizables = Lists.newArrayList();
    authorizables.add(new Collection(""));
    assertEquals(tag, HierarchyStore.getHierarchyTag(component, authorizables));

    authorizables.add(new Field(""));
    assertEquals(tag, HierarchyStore.getHierarchyTag(component, authorizables));
  }

  @Test(expected=IllegalStateException.class)
  public void testSearchHierarchyException() throws Exception {
    String component = "SOLR";
    List<Authorizable> authorizables = Lists.newArrayList();
    authorizables.add(new Authorizable() {
      @Override
      public String getTypeName() {
        return "not" + SearchModelAuthorizable.AuthorizableType.Collection.name();
      }
      @Override
      public String getName() {
        return "";
      }
    });
    HierarchyStore.getHierarchyTag(component, authorizables);
  }
}
