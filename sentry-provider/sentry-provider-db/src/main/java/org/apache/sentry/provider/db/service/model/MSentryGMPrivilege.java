/**
vim  * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.sentry.provider.db.service.model;

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.KV_JOINER;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jdo.annotations.PersistenceCapable;
import org.apache.sentry.core.common.Authorizable;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Database backed Sentry Generic Privilege for new authorization Model
 * Any changes to this object
 * require re-running the maven build so DN an re-enhance.
 */
@PersistenceCapable
public class MSentryGMPrivilege {
  private static final String PREFIX_RESOURCE_NAME = "resourceName";
  private static final String PREFIX_RESOURCE_TYPE = "resourceType";
  private static final String NULL_COL = "__NULL__";
  private static final int AUTHORIABLE_LEVEL = 4;
  /**
   * The authorizable List has been stored into resourceName and resourceField columns
   * We assume that the generic model privilege for any component(hive/impala or solr) doesn't exceed four level.
   * This generic model privilege currently can support maximum 4 level.
   **/
  private String resourceName0 = NULL_COL;
  private String resourceType0 = NULL_COL;
  private String resourceName1 = NULL_COL;
  private String resourceType1 = NULL_COL;
  private String resourceName2 = NULL_COL;
  private String resourceType2 = NULL_COL;
  private String resourceName3 = NULL_COL;
  private String resourceType3 = NULL_COL;

  private String serviceName = "";
  private String componentName = "";
  private String action = "";
  private String scope;

  private Boolean grantOption = false;
  // roles this privilege is a part of
  private Set<MSentryRole> roles;
  private long createTime;
  private String grantorPrincipal;

  public MSentryGMPrivilege() {
    this.roles = new HashSet<MSentryRole>();
  }

  public MSentryGMPrivilege(String serviceName, String componentName, String action,
                                 String grantorPrincipal,List<? extends Authorizable> authorizables,
                                 String scope,Boolean grantOption) {
    this.serviceName = serviceName;
    this.componentName = componentName;
    this.action = action;
    this.grantorPrincipal = grantorPrincipal;
    this.grantOption = grantOption;
    this.scope = scope;
    this.roles = new HashSet<MSentryRole>();
    this.createTime = System.currentTimeMillis();
    setAuthorizables(authorizables);
  }

  public MSentryGMPrivilege(MSentryGMPrivilege copy) {
    this.action = copy.action;
    this.componentName = copy.componentName;
    this.serviceName = copy.serviceName;
    this.grantOption = copy.grantOption;
    this.scope = copy.scope;
    this.grantorPrincipal = copy.grantorPrincipal;
    this.createTime = copy.createTime;
    setAuthorizables(copy.getAuthorizables());
    this.roles = new HashSet<MSentryRole>();
    for (MSentryRole role : copy.roles) {
      roles.add(role);
    }
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getComponentName() {
    return componentName;
  }

  public void setComponentName(String componentName) {
    this.componentName = componentName;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public Boolean getGrantOption() {
    return grantOption;
  }

  public void setGrantOption(Boolean grantOption) {
    this.grantOption = grantOption;
  }

  public Set<MSentryRole> getRoles() {
    return roles;
  }

  public void setRoles(Set<MSentryRole> roles) {
    this.roles = roles;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public String getGrantorPrincipal() {
    return grantorPrincipal;
  }

  public void setGrantorPrincipal(String grantorPrincipal) {
    this.grantorPrincipal = grantorPrincipal;
  }

  public String getScope() {
    return scope;
  }

  public void setScope(String scope) {
    this.scope = scope;
  }

  public List<? extends Authorizable> getAuthorizables() {
    List<Authorizable> authorizables = Lists.newArrayList();
    //construct atuhorizable lists
    for (int i = 0; i < AUTHORIABLE_LEVEL; i++) {
      final String resourceName = (String) getField(this, PREFIX_RESOURCE_NAME + String.valueOf(i));
      final String resourceTYpe = (String) getField(this, PREFIX_RESOURCE_TYPE + String.valueOf(i));

      if (notNULL(resourceName) && notNULL(resourceTYpe)) {
        authorizables.add(new Authorizable() {
          @Override
          public String getTypeName() {
            return resourceTYpe;
          }
          @Override
          public String getName() {
            return resourceName;
          }
        });
      }
    }
    return authorizables;
  }

  public void setAuthorizables(List<? extends Authorizable> authorizables) {
    if ((authorizables == null) || (authorizables.isEmpty())) {
      return;
    }
    if (authorizables.size() > AUTHORIABLE_LEVEL) {
      throw new IllegalStateException("This generic privilege model only supports maximum 4 level.");
    }

    for (int i = 0; i < authorizables.size(); i++) {
      setField(this, PREFIX_RESOURCE_NAME + String.valueOf(i), toNULLCol(authorizables.get(i).getName()));
      setField(this, PREFIX_RESOURCE_TYPE + String.valueOf(i), toNULLCol(authorizables.get(i).getTypeName()));
    }
  }

  public void appendRole(MSentryRole role) {
    if (roles.add(role)) {
      role.appendGMPrivilege(this);
    }
  }

  public void removeRole(MSentryRole role) {
    if(roles.remove(role)) {
      role.removeGMPrivilege(this);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((action == null) ? 0 : action.hashCode());
    result = prime * result + ((componentName == null) ? 0 : componentName.hashCode());
    result = prime * result + ((serviceName == null) ? 0 : serviceName.hashCode());
    result = prime * result + ((grantOption == null) ? 0 : grantOption.hashCode());
    result = prime * result + ((scope == null) ? 0 : scope.hashCode());

    for (Authorizable authorizable : getAuthorizables()) {
      result = prime * result + authorizable.getName().hashCode();
      result = prime * result + authorizable.getTypeName().hashCode();
    }

    return result;
  }

  @Override
  public String toString() {
    List<String> unifiedNames = Lists.newArrayList();
    for (Authorizable auth : getAuthorizables()) {
      unifiedNames.add(KV_JOINER.join(auth.getTypeName(),auth.getName()));
    }

    return "MSentryGMPrivilege ["
        + ", serverName=" + serviceName + ", componentName=" + componentName
        + ", authorizables=" + AUTHORIZABLE_JOINER.join(unifiedNames)+ ", scope=" + scope
        + ", action=" + action + ", roles=[...]"  + ", createTime="
        + createTime + ", grantorPrincipal=" + grantorPrincipal
        + ", grantOption=" + grantOption +"]";
  }

  @Override
  public boolean equals(Object obj) {
      if (this == obj)
          return true;
      if (obj == null)
          return false;
      if (getClass() != obj.getClass())
          return false;
      MSentryGMPrivilege other = (MSentryGMPrivilege) obj;
      if (action == null) {
          if (other.action != null)
              return false;
      } else if (!action.equalsIgnoreCase(other.action))
          return false;
      if (scope == null) {
        if (other.scope != null)
            return false;
      } else if (!scope.equalsIgnoreCase(other.scope))
        return false;
      if (serviceName == null) {
          if (other.serviceName != null)
              return false;
      } else if (!serviceName.equalsIgnoreCase(other.serviceName))
          return false;
      if (componentName == null) {
          if (other.componentName != null)
              return false;
      } else if (!componentName.equalsIgnoreCase(other.componentName))
          return false;
      if (grantOption == null) {
        if (other.grantOption != null)
          return false;
      } else if (!grantOption.equals(other.grantOption))
        return false;

      List<? extends Authorizable> authorizables = getAuthorizables();
      List<? extends Authorizable> other_authorizables = other.getAuthorizables();

      if (authorizables.size() != other_authorizables.size()) {
        return false;
      }
      for (int i = 0; i < authorizables.size(); i++) {
        String o1 = KV_JOINER.join(authorizables.get(i).getTypeName(),
                                         authorizables.get(i).getName());
        String o2 = KV_JOINER.join(other_authorizables.get(i).getTypeName(),
            other_authorizables.get(i).getName());
        if (!o1.equalsIgnoreCase(o2)) {
          return false;
        }
      }
      return true;
  }

  public static String toNULLCol(String col) {
    return Strings.isNullOrEmpty(col) ? NULL_COL : col;
  }

  public static boolean notNULL(String s) {
    return !(Strings.isNullOrEmpty(s) || NULL_COL.equals(s));
  }

  public static <T> void setField(Object obj, String fieldName, T fieldValue) {
    try {
      Class<?> clazz = obj.getClass();
      Field field=clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(obj, fieldValue);
    } catch (Exception e) {
      throw new RuntimeException("setField error: " + e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T getField(Object obj, String fieldName) {
    try {
      Class<?> clazz = obj.getClass();
      Field field=clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (T)field.get(obj);
    } catch (Exception e) {
      throw new RuntimeException("getField error: " + e.getMessage(), e);
    }
  }

  /**
   * get the filter include all fields of MSentryGMPrivilege
   * @param privilege
   * @return filter
   */
  public static String getEntireFilter(MSentryGMPrivilege privilege) {
    StringBuilder entireFilter = new StringBuilder();
    entireFilter.append("serviceName == \"" + toNULLCol(privilege.getServiceName()) + "\" ");
    entireFilter.append("&& componentName == \"" + toNULLCol(privilege.getComponentName()) + "\" ");
    entireFilter.append("&& scope == \"" + toNULLCol(privilege.getScope()) + "\" ");
    entireFilter.append("&& action == \"" + toNULLCol(privilege.getAction()) + "\"");
    if (privilege.getGrantOption() == null) {
      entireFilter.append("&& this.grantOption == null ");
    } else if (privilege.getGrantOption()) {
      entireFilter.append("&& grantOption ");
    } else {
      entireFilter.append("&& !grantOption ");
    }
    List<? extends Authorizable> authorizables = privilege.getAuthorizables();
    for (int i = 0; i < AUTHORIABLE_LEVEL; i++) {
      String resourceName = PREFIX_RESOURCE_NAME + String.valueOf(i);
      String resourceType = PREFIX_RESOURCE_TYPE + String.valueOf(i);

      if (i >= authorizables.size()) {
        entireFilter.append("&& " + resourceName + " == \"" + NULL_COL + "\" ");
        entireFilter.append("&& " + resourceType + " == \"" + NULL_COL + "\" ");
      } else {
        entireFilter.append("&& " + resourceName + " == \"" + authorizables.get(i).getName() + "\" ");
        entireFilter.append("&& " + resourceType + " == \"" + authorizables.get(i).getTypeName() + "\" ");
      }
    }
    return entireFilter.toString();
  }

  /**
   * get the filter include partial fields of MSentryGMPrivilege
   * @param privilege
   * @return filter
   */
  public static String getPartialFilter(MSentryGMPrivilege privilege) {
    StringBuilder partialFilter = new StringBuilder();
    partialFilter.append("serviceName == \"" + toNULLCol(privilege.getServiceName()) + "\" ");
    partialFilter.append("&& componentName == \"" + toNULLCol(privilege.getComponentName()) + "\" ");

    if (privilege.getGrantOption() != null) {
      if (privilege.getGrantOption()) {
        partialFilter.append("&& grantOption ");
      } else {
        partialFilter.append("&& !grantOption ");
      }
    }

    List<? extends Authorizable> authorizables = privilege.getAuthorizables();
    for (int i = 0; i < AUTHORIABLE_LEVEL; i++) {
      String resourceName = PREFIX_RESOURCE_NAME + String.valueOf(i);
      String resourceType = PREFIX_RESOURCE_TYPE + String.valueOf(i);
      if (i < authorizables.size()) {
        partialFilter.append("&& " + resourceName + " == \"" + authorizables.get(i).getName() + "\" ");
        partialFilter.append("&& " + resourceType + " == \"" + authorizables.get(i).getTypeName() + "\" ");
      }
    }

    return partialFilter.toString();
  }
}
