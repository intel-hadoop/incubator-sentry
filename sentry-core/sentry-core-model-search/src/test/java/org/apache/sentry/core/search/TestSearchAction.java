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
package org.apache.sentry.core.search;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActionReader;
import org.apache.sentry.core.common.CompoundAction;
import org.apache.sentry.core.model.search.SearchAction;
import org.apache.sentry.core.model.search.SearchActionReader;
import org.apache.sentry.core.model.search.SearchConstants;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.apache.sentry.core.model.search.SearchAction.ALL;
import static org.apache.sentry.core.model.search.SearchAction.UPDATE;
import static org.apache.sentry.core.model.search.SearchAction.QUERY;

public class TestSearchAction {
  private final ActionReader READER = new SearchActionReader();

  @Test
  public void testDeserialize() throws Exception {
    assertEquals(QUERY.getAction(), READER.deserialize(SearchConstants.QUERY));
    assertEquals(UPDATE.getAction(), READER.deserialize(SearchConstants.UPDATE));
    assertEquals(ALL.getAction(), READER.deserialize(SearchConstants.ALL));
  }

  @Test
  public void testIncludeAndLeft() throws Exception {
    CompoundAction all = (CompoundAction)ALL.getAction();
    assertEquals(Lists.newArrayList(QUERY, UPDATE), all.getActions());
    assertTrue(all.includeAction(SearchAction.QUERY));
    assertTrue(all.includeAction(SearchAction.UPDATE));
    assertEquals(Lists.newArrayList(UPDATE), all.leftActions(QUERY));
    assertEquals(Lists.newArrayList(QUERY), all.leftActions(UPDATE));
  }
}
