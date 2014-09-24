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
package org.apache.sentry.core.model.search;

import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.BaseAction;
import org.apache.sentry.core.common.CompoundAction;

public enum SearchAction implements Action {

  UPDATE(new BaseAction(SearchConstants.UPDATE)),
  QUERY(new BaseAction(SearchConstants.QUERY)),
  ALL(new CompoundAction.Builder()
      .setName(Action.ALL)
      .addAction(QUERY)
      .addAction(UPDATE)
      .build());

  private Action action;

  private SearchAction(Action action) {
    this.action = action;
  }

  public Action getAction() {
    return action;
  }

  @Override
  public String getValue() {
    return action.getValue();
  }

}
