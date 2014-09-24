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
package org.apache.sentry.core.common;

import java.util.List;

import com.google.common.collect.Lists;

public class CompoundAction extends BaseAction {

  private List<? extends Action> actions;

  private CompoundAction(String value, List<? extends Action> actions) {
    super(value);
    this.actions = actions;
  }

  public boolean includeAction(Action ac) {
    for (Action action : actions) {
      if (action.getValue().equalsIgnoreCase(ac.getValue())) {
        return true;
      }
    }
    return false;
  }

  public List<? extends Action> leftActions(Action ac) {
    List<Action> leftActions = Lists.newArrayList();
    for (Action action : actions) {
      if (!action.getValue().equalsIgnoreCase(ac.getValue())) {
        leftActions.add(action);
      }
    }
    return leftActions;
  }

  public List<? extends Action> getActions() {
    return actions;
  }

  public static class Builder {
    private List<Action> actions = Lists.newArrayList();
    private String name;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder addAction(Action action) {
      this.actions.add(action);
      return this;
    }

    public CompoundAction build() {
      return new CompoundAction(name, actions);
    }
  }

}
