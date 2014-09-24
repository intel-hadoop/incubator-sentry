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

import static org.apache.sentry.provider.common.ProviderConstants.KV_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_JOINER;

import java.util.ArrayList;
import java.util.List;
import org.apache.sentry.core.common.Authorizable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class PrivilegeObject {
  private final String component;
  private final String service;
  private final String action;
  private final String scope;
  private final Boolean grantOption;
  private List<? extends Authorizable> authorizables;

  private PrivilegeObject(String component, String service, String action,
      String scope, Boolean grantOption,
      List<? extends Authorizable> authorizables) {
    this.component = component;
    this.service = service;
    this.action = action;
    this.scope = scope;
    this.grantOption = grantOption;
    this.authorizables = authorizables;
  }

  public List<? extends Authorizable> getAuthorizables() {
    return authorizables;
  }

  public String getAction() {
    return action;
  }

  public String getComponent() {
    return component;
  }

  public String getService() {
    return service;
  }

  public String getScope() {
    return scope;
  }

  public Boolean getGrantOption() {
    return grantOption;
  }

  @Override
  public String toString() {
    List<String> authorizable = Lists.newArrayList();
    for (Authorizable az : authorizables) {
      authorizable.add(KV_JOINER.join(az.getTypeName(),az.getName()));
    }
    return "PrivilegeObject [" + ", service=" + service + ", component="
        + component + ", authorizables=" + AUTHORIZABLE_JOINER.join(authorizable)+ ", scope=" + scope
        + ", action=" + action + ", grantOption=" + grantOption + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((action == null) ? 0 : action.hashCode());
    result = prime * result + ((component == null) ? 0 : component.hashCode());
    result = prime * result + ((service == null) ? 0 : service.hashCode());
    result = prime * result
        + ((grantOption == null) ? 0 : grantOption.hashCode());
    result = prime * result + ((scope == null) ? 0 : scope.hashCode());

    for (Authorizable authorizable : authorizables) {
      result = prime * result + authorizable.getTypeName().hashCode();
      result = prime * result + authorizable.getName().hashCode();
    }

    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PrivilegeObject other = (PrivilegeObject) obj;
    if (action == null) {
      if (other.action != null)
        return false;
    } else if (!action.equals(other.action))
      return false;
    if (scope == null) {
      if (other.scope != null)
        return false;
    } else if (!scope.equals(other.scope))
      return false;
    if (service == null) {
      if (other.service != null)
        return false;
    } else if (!service.equals(other.service))
      return false;
    if (component == null) {
      if (other.component != null)
        return false;
    } else if (!component.equals(other.component))
      return false;
    if (grantOption == null) {
      if (other.grantOption != null)
        return false;
    } else if (!grantOption.equals(other.grantOption))
      return false;

    if (authorizables.size() != other.authorizables.size()) {
      return false;
    }
    for (int i = 0; i < authorizables.size(); i++) {
      String o1 = KV_JOINER.join(authorizables.get(i).getTypeName(),
          authorizables.get(i).getName());
      String o2 = KV_JOINER.join(other.authorizables.get(i).getTypeName(),
          other.authorizables.get(i).getName());
      if (!o1.equalsIgnoreCase(o2)) {
        return false;
      }
    }
    return true;
  }

  public static class Builder {
    private String component;
    private String service;
    private String action;
    private String scope;
    private Boolean grantOption;
    private List<? extends Authorizable> authorizables;

    public Builder() {

    }

    public Builder(PrivilegeObject privilege) {
      this.component = privilege.component;
      this.service = privilege.service;
      this.action = privilege.action;
      this.scope = privilege.scope;
      this.grantOption = privilege.grantOption;
      this.authorizables = privilege.authorizables;
    }

    public Builder setComponent(String component) {
      this.component = component;
      return this;
    }

    public Builder setService(String service) {
      this.service = service;
      return this;
    }

    public Builder setAction(String action) {
      this.action = action;
      return this;
    }

    public Builder setScope(String scope) {
      this.scope = scope;
      return this;
    }

    public Builder withGrantOption(Boolean grantOption) {
      this.grantOption = grantOption;
      return this;
    }

    public Builder setAuthorizables(List<? extends Authorizable> authorizables) {
      this.authorizables = authorizables;
      return this;
    }

    public PrivilegeObject build() {
      Preconditions.checkNotNull(component);
      Preconditions.checkNotNull(service);
      Preconditions.checkNotNull(scope);
      Preconditions.checkNotNull(authorizables);
      Preconditions.checkNotNull(action);
      //CaseInsensitive authorizable name
      List<Authorizable> newAuthorizable = Lists.newArrayList();
      for (final Authorizable authorizable : authorizables) {
        newAuthorizable.add(new Authorizable() {
          @Override
          public String getTypeName() {
            return authorizable.getTypeName();
          }
          @Override
          public String getName() {
            return authorizable.getName().toLowerCase();
          }
        });
      }
      return new PrivilegeObject(component.toLowerCase(),
                                     service.toLowerCase(),
                                     action.toLowerCase(),
                                     scope.toLowerCase(),
                                     grantOption,
                                     newAuthorizable);
    }
  }
}
