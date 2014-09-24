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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.CompoundAction;
import org.apache.sentry.provider.db.genericModel.service.thrift.PrivilegeObject;
import org.apache.sentry.provider.db.genericModel.service.thrift.PrivilegeObject.Builder;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

/**
 * The SeperatePrivSentryStore will store the authorizables from {@l
 * into separated column. Take the authorizables:[DATABASE=db1,TABLE=tb1,COLUMN=cl1] for example,
 * The DATABASE,db1,TABLE,tb1,COLUMN and cl1 will be stored into the six columns(resourceName0=db1,resourceType0=DATABASE,
 * resourceName1=tb1,resourceType1=TABLE,
 * resourceName2=cl1,resourceType2=COLUMN ) of generic privilege table
 */
public class GMPrivilegeSentryStore extends SentryStoreBase {

  public GMPrivilegeSentryStore(Configuration conf) throws Exception{
    super(conf);
  }

  @Override
  protected void grantPrivilege(PrivilegeObject privilege, String grantorPrincipal,
      MSentryRole role, PersistenceManager pm) throws SentryUserException {
    MSentryGMPrivilege mPrivilege = convertToPrivilege(privilege, grantorPrincipal);
    grantRolePartial(mPrivilege, role, pm);
  }

  private void grantRolePartial(MSentryGMPrivilege grantPrivilege,
      MSentryRole role,PersistenceManager pm) {
    /**
     * If Grant is for ALL action and other actions belongs to ALL action already exists..
     * need to remove it and GRANT ALL action
     */
    Action action = getAction(grantPrivilege.getComponentName(), grantPrivilege.getAction());
    Action allAction = getAction(grantPrivilege.getComponentName(), Action.ALL);

    if (action.equals(allAction)) {
      /**
       * allAction is a compound action that includes some baseAction such as INSERT,SELECT and CREATE.
       */
      CompoundAction cpAction = (CompoundAction)allAction;
      for (Action ac : cpAction.getActions()) {
        grantPrivilege.setAction(ac.getValue());
        MSentryGMPrivilege existPriv = getPrivilege(grantPrivilege, pm);
        if ((existPriv != null) && (role.getGmPrivileges().contains(existPriv))) {
          /**
           * force to load all roles related this privilege
           * avoid the lazy-loading risk,such as:
           * if the roles field of privilege aren't loaded, then the roles is a empty set
           * privilege.removeRole(role) and pm.makePersistent(privilege)
           * will remove other roles that shouldn't been removed
           */
          pm.retrieve(existPriv);
          existPriv.removeRole(role);
          pm.makePersistent(existPriv);
        }
      }
    } else {
      /**
       * If ALL Action already exists..
       * do nothing.
       */
      grantPrivilege.setAction(allAction.getValue());
      MSentryGMPrivilege allPrivilege = getPrivilege(grantPrivilege, pm);
      if ((allPrivilege != null) && (role.getGmPrivileges().contains(allPrivilege))) {
        return;
      }
    }

    /**
     * restore the action
     */
    grantPrivilege.setAction(action.getValue());
    /**
     * check the privilege is exist or not
     */
    MSentryGMPrivilege mPrivilege = getPrivilege(grantPrivilege, pm);
    if (mPrivilege == null) {
      mPrivilege = grantPrivilege;
    }
    mPrivilege.appendRole(role);
    pm.makePersistent(mPrivilege);
  }

  @Override
  protected void revokePrivilege(PrivilegeObject privilege, String grantorPrincipal,
      MSentryRole role, PersistenceManager pm) throws SentryUserException {
    MSentryGMPrivilege mPrivilege = getPrivilege(convertToPrivilege(privilege,grantorPrincipal), pm);
    if (mPrivilege == null) {
      mPrivilege = convertToPrivilege(privilege,grantorPrincipal);
    }

    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();
    privilegeGraph.addAll(populateIncludePrivileges(Sets.newHashSet(role), mPrivilege, pm));

    /**
     * Get the privilege graph
     * populateIncludePrivileges will get the privileges that needed revoke
     */
    for (MSentryGMPrivilege persistedPriv : privilegeGraph) {
      /**
       * force to load all roles related this privilege
       * avoid the lazy-loading risk,such as:
       * if the roles field of privilege aren't loaded, then the roles is a empty set
       * privilege.removeRole(role) and pm.makePersistent(privilege)
       * will remove other roles that shouldn't been removed
       */
      pm.retrieve(persistedPriv);

      revokeRolePartial(mPrivilege, persistedPriv, role, pm);
    }
  }

  /**
   * Explore Privilege graph and collect privileges that are belong to the specific privilege
   */
  @SuppressWarnings("unchecked")
  private Set<MSentryGMPrivilege> populateIncludePrivileges(Set<MSentryRole> roles,
      MSentryGMPrivilege parent, PersistenceManager pm) {
    Set<MSentryGMPrivilege> childrens = Sets.newHashSet();

    Query query = pm.newQuery(MSentryGMPrivilege.class);
    StringBuilder filters = new StringBuilder();
    // add filter for privilege fields
    filters.append(MSentryGMPrivilege.getPartialFilter(parent));
    // add filter for role names
    if ((roles != null) && (roles.size() > 0)) {
      filters.append("&& " + getRolesFilter(roles, query));
    }
    query.setFilter(filters.toString());

    List<MSentryGMPrivilege> privileges = (List<MSentryGMPrivilege>)query.execute();
    childrens.addAll(privileges);
    return childrens;
  }

  /**
   * Roles can be granted compound action like ALL privilege on resource object.
   * Take solr component for example, When a role has been granted ALL action but
   * QUERY or UPDATE or CREATE are revoked, we need to remove the ALL
   * privilege and add left privileges like UPDATE and CREATE(QUERY was revoked) or
   * QUERY and UPDATE(CREATEE was revoked).
   */
  private void revokeRolePartial(MSentryGMPrivilege revokePrivilege,
      MSentryGMPrivilege persistedPriv, MSentryRole role,
      PersistenceManager pm) {
    Action revokeaction = getAction(revokePrivilege.getComponentName(), revokePrivilege.getAction());
    Action persistedAction = getAction(persistedPriv.getComponentName(), persistedPriv.getAction());
    Action allAction = getAction(revokePrivilege.getComponentName(), Action.ALL);

    if (revokeaction.equals(allAction)) {
      /**
       * if revoke action is ALL, directly revoke its children privileges and itself
       */
      persistedPriv.removeRole(role);
      pm.makePersistent(persistedPriv);
    } else {
      /**
       * if persisted action is ALL, it only revoke the requested action and left partial actions
       * like the requested action is SELECT, the UPDATE and CREATE action are left
       */
      if (persistedAction.equals(allAction)) {
        /**
         * revoke the ALL privilege
         */
        CompoundAction cpAction = (CompoundAction)allAction;
        persistedPriv.removeRole(role);
        pm.makePersistent(persistedPriv);
        /**
         * grant the left privileges to role
         */
        for (Action leftAction: cpAction.leftActions(revokeaction)) {
          MSentryGMPrivilege leftPriv = new MSentryGMPrivilege(persistedPriv);
          leftPriv.setAction(leftAction.getValue());
          leftPriv.appendRole(role);
          pm.makePersistent(leftPriv);
        }
      } else if (revokeaction.equals(persistedAction)) {
        /**
         * if the revoke action is equal to the persisted action and they aren't ALL action
         * directly remove the role from privilege
         */
        persistedPriv.removeRole(role);
        pm.makePersistent(persistedPriv);
      } else {
        /**
         * if the revoke action is not equal to the persisted action,
         * do nothing
         */
      }
    }
  }

  /**
   * drop any role related to the requested privilege and its children privileges
   */
  @Override
  protected void dropPrivilege(PrivilegeObject privilege, String grantorPrincipal,
      PersistenceManager pm) {
    MSentryGMPrivilege requestPrivilege = convertToPrivilege(privilege,grantorPrincipal);

    if (Strings.isNullOrEmpty(privilege.getAction())) {
      requestPrivilege.setAction(getAction(privilege.getComponent(), Action.ALL).getValue());
    }
    /**
     * Get the privilege graph
     * populateIncludePrivileges will get the privileges that need dropped,
     */
    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();
    privilegeGraph.addAll(populateIncludePrivileges(null, requestPrivilege, pm));

    for (MSentryGMPrivilege mPrivilege : privilegeGraph) {
      /**
       * force to load all roles related this privilege
       * avoid the lazy-loading
       */
      pm.retrieve(mPrivilege);
      Set<MSentryRole> roles = mPrivilege.getRoles();
      for (MSentryRole role : roles) {
        revokeRolePartial(requestPrivilege, mPrivilege, role, pm);
      }
    }
  }

  private MSentryGMPrivilege convertToPrivilege(
      PrivilegeObject privilege, String grantorPrincipal) {
    return new MSentryGMPrivilege(privilege.getService(),
        privilege.getComponent(), privilege.getAction(),
        grantorPrincipal, privilege.getAuthorizables(),
        privilege.getScope(), privilege.getGrantOption());
  }

  private MSentryGMPrivilege getPrivilege(MSentryGMPrivilege privilege, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryGMPrivilege.class);
    query.setFilter(MSentryGMPrivilege.getEntireFilter(privilege));
    query.setUnique(true);
    return (MSentryGMPrivilege)query.execute(privilege.getGrantOption());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Set<PrivilegeObject> getPrivilegesByRole(Set<String> roleNames, PersistenceManager pm) {
    Set<PrivilegeObject> privileges = Sets.newHashSet();
    if ((roleNames == null) || (roleNames.size() == 0)) {
      return privileges;
    }
    Query query = pm.newQuery(MSentryGMPrivilege.class);
    StringBuilder filters = new StringBuilder();
    // add filter for role names
    filters.append(getRoleNamesFilter(roleNames, query));

    query.setFilter(filters.toString());
    List<MSentryGMPrivilege> mPrivileges = (List<MSentryGMPrivilege>) query.execute();
    if ((mPrivileges == null) || (mPrivileges.size() ==0)) {
      return privileges;
    }
    for (MSentryGMPrivilege mPrivilege : mPrivileges) {
      privileges.add(new Builder()
                               .setComponent(mPrivilege.getComponentName())
                               .setService(mPrivilege.getServiceName())
                               .setAction(mPrivilege.getAction())
                               .setAuthorizables(mPrivilege.getAuthorizables())
                               .setScope(mPrivilege.getScope())
                               .withGrantOption(mPrivilege.getGrantOption())
                               .build());
    }
    return privileges;
  }

  @Override
  protected Set<PrivilegeObject> getPrivilegesByProvider(String component,
      String service, Set<String> roleNames,
      List<? extends Authorizable> authorizables, PersistenceManager pm) {

    Set<PrivilegeObject> privileges = Sets.newHashSet();

    Set<MSentryRole> roles = Sets.newHashSet();
    for (String roleName : roleNames) {
      MSentryRole role = getRole(roleName, pm);
      if (role != null) {
        roles.add(role);
      }
    }
    MSentryGMPrivilege parentPrivilege = new MSentryGMPrivilege(service, component, null, null,
                                   authorizables, null, null);
    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();
    privilegeGraph.addAll(populateIncludePrivileges(roles, parentPrivilege, pm));

    for (MSentryGMPrivilege mPrivilege : privilegeGraph) {
      privileges.add(new Builder()
                               .setComponent(mPrivilege.getComponentName())
                               .setService(mPrivilege.getServiceName())
                               .setAction(mPrivilege.getAction())
                               .setAuthorizables(mPrivilege.getAuthorizables())
                               .setScope(mPrivilege.getScope())
                               .withGrantOption(mPrivilege.getGrantOption())
                               .build());
    }
    return privileges;
  }

  @Override
  protected void renamePrivilege(String component, String service,
      List<? extends Authorizable> oldAuthorizables, List<? extends Authorizable> newAuthorizables,
      String grantorPrincipal, PersistenceManager pm)
      throws SentryUserException {
    MSentryGMPrivilege oldPrivilege = new MSentryGMPrivilege(service,
        component, null, grantorPrincipal, oldAuthorizables, null, null);
    oldPrivilege.setAction(getAction(component,Action.ALL).getValue());
    /**
     * Get the privilege graph
     * populateIncludePrivileges will get the old privileges that need dropped
     */
    Set<MSentryGMPrivilege> privilegeGraph = Sets.newHashSet();
    privilegeGraph.addAll(populateIncludePrivileges(null, oldPrivilege, pm));

    for (MSentryGMPrivilege dropPrivilege : privilegeGraph) {
      /**
       * construct the new privilege needed to add
       */
      List<Authorizable> authorizables = new ArrayList<Authorizable>(
          dropPrivilege.getAuthorizables());
      for (int i = 0; i < newAuthorizables.size(); i++) {
        authorizables.set(i, newAuthorizables.get(i));
      }
      MSentryGMPrivilege newPrivilge = new MSentryGMPrivilege(
          service, component,
          dropPrivilege.getAction(), grantorPrincipal,
          authorizables, dropPrivilege.getScope(), dropPrivilege.getGrantOption());

      /**
       * force to load all roles related this privilege
       * avoid the lazy-loading
       */
      pm.retrieve(dropPrivilege);

      Set<MSentryRole> roles = dropPrivilege.getRoles();
      for (MSentryRole role : roles) {
        revokeRolePartial(oldPrivilege, dropPrivilege, role, pm);
        grantRolePartial(newPrivilge, role, pm);
      }
    }
  }
}
