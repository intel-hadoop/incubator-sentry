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
package org.apache.sentry.core.model.db;

import java.util.List;

public class PrivilegeInfo {
  private String privilegeScope;
  private String serverName;
  private String dbName;
  private String tableOrViewName;
  private List<String> colums;
  private String uri;
  private String action;
  private Boolean grantOption;

  public static class Builder {
    private String privilegeScope;
    private String serverName;
    private String dbName;
    private String tableOrViewName;
    private List<String> colums;
    private String uri;
    private String action;
    private Boolean grantOption;

    public Builder setPrivilegeScope(String privilegeScope) {
      this.privilegeScope = privilegeScope;
      return this;
    }

    public Builder setServerName(String serverName) {
      this.serverName = serverName;
      return this;
    }

    public Builder setDbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public Builder setTableOrViewName(String tableOrViewName) {
      this.tableOrViewName = tableOrViewName;
      return this;
    }

    public Builder setColumns(List<String> colums) {
      this.colums = colums;
      return this;
    }

    public Builder setURI(String uri) {
      this.uri = uri;
      return this;
    }

    public Builder setAction(String action) {
      this.action = action;
      return this;
    }

    public Builder setGrantOption(Boolean grantOption) {
      this.grantOption = grantOption;
      return this;
    }

    public PrivilegeInfo build() {
      return new PrivilegeInfo(this);
    }
  }

  public PrivilegeInfo(Builder builder) {
    this.privilegeScope = builder.privilegeScope;
    this.serverName = builder.serverName;
    this.dbName = builder.dbName;
    this.tableOrViewName = builder.tableOrViewName;
    this.colums = builder.colums;
    this.uri = builder.uri;
    this.action = builder.action;
    this.grantOption = builder.grantOption;
  }

  public String getPrivilegeScope() {
    return privilegeScope;
  }

  public String getServerName() {
    return serverName;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableOrViewName() {
    return tableOrViewName;
  }

  public List<String> getColumns() {
    return colums;
  }

  public String getURI() {
    return uri;
  }

  public String getAction() {
    return action;
  }

  public Boolean getGrantOption() {
    return grantOption;
  }

}