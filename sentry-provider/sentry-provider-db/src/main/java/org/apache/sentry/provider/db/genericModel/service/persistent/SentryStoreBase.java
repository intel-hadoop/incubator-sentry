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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActionReader;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.search.SearchActionReader;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.genericModel.service.thrift.PrivilegeObject;
import org.apache.sentry.provider.db.genericModel.service.thrift.PrivilegeUtil;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.model.MSentryVersion;
import org.apache.sentry.provider.db.service.persistent.CommitContext;
import org.apache.sentry.provider.db.service.persistent.SentryStoreSchemaInfo;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessor;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class SentryStoreBase implements SentryStoreLayer {

  private static final UUID SERVER_UUID = UUID.randomUUID();
  /**
   * Commit order sequence id. This is used by notification handlers
   * to know the order in which events where committed to the database.
   * This instance variable is incremented in incrementGetSequenceId
   * and read in commitUpdateTransaction. Synchronization on this
   * is required to read commitSequenceId.
   */
  private long commitSequenceId;
  private final PersistenceManagerFactory pmf;
  private final Set<String> adminGroups;
  private final Configuration conf;
  private static final Map<String, ActionReader> actionReaders = Maps.newHashMap();
  static{
    actionReaders.put("solr", new SearchActionReader());
  }

  public SentryStoreBase(Configuration conf) throws SentryUserException {
    this.conf = conf;
    commitSequenceId = 0;
    Properties prop = new Properties();
    prop.putAll(ServerConfig.SENTRY_STORE_DEFAULTS);
    String jdbcUrl = conf.get(ServerConfig.SENTRY_STORE_JDBC_URL, "").trim();
    Preconditions.checkArgument(!jdbcUrl.isEmpty(), "Required parameter " +
        ServerConfig.SENTRY_STORE_JDBC_URL + " missing");
    String user = conf.get(ServerConfig.SENTRY_STORE_JDBC_USER, ServerConfig.
        SENTRY_STORE_JDBC_USER_DEFAULT).trim();
    String pass = conf.get(ServerConfig.SENTRY_STORE_JDBC_PASS, ServerConfig.
        SENTRY_STORE_JDBC_PASS_DEFAULT).trim();
    String driverName = conf.get(ServerConfig.SENTRY_STORE_JDBC_DRIVER,
        ServerConfig.SENTRY_STORE_JDBC_DRIVER_DEFAULT);
    prop.setProperty(ServerConfig.JAVAX_JDO_URL, jdbcUrl);
    prop.setProperty(ServerConfig.JAVAX_JDO_USER, user);
    prop.setProperty(ServerConfig.JAVAX_JDO_PASS, pass);
    prop.setProperty(ServerConfig.JAVAX_JDO_DRIVER_NAME, driverName);
    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      if (key.startsWith(ServerConfig.SENTRY_JAVAX_JDO_PROPERTY_PREFIX) ||
          key.startsWith(ServerConfig.SENTRY_DATANUCLEUS_PROPERTY_PREFIX)) {
        key = StringUtils.removeStart(key, ServerConfig.SENTRY_DB_PROPERTY_PREFIX);
        prop.setProperty(key, entry.getValue());
      }
    }

    boolean checkSchemaVersion = conf.get(
        ServerConfig.SENTRY_VERIFY_SCHEM_VERSION,
        ServerConfig.SENTRY_VERIFY_SCHEM_VERSION_DEFAULT).equalsIgnoreCase(
        "true");

    if (!checkSchemaVersion) {
      prop.setProperty("datanucleus.autoCreateSchema", "true");
      prop.setProperty("datanucleus.fixedDatastore", "false");
    }
    pmf = JDOHelper.getPersistenceManagerFactory(prop);
    verifySentryStoreSchema(conf, checkSchemaVersion);
    adminGroups = ImmutableSet.copyOf(toTrimedLower(Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}))));
  }

  private Set<String> getRequestorGroups(String userName)
      throws SentryUserException {
    return SentryPolicyStoreProcessor.getGroupsFromUserName(this.conf, userName);
  }

  private Set<String> toTrimedLower(Set<String> s) {
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }

  private String toTrimedLower(String s) {
    return s.trim().toLowerCase();
  }


  // ensure that the backend DB schema is set
  private void verifySentryStoreSchema(Configuration serverConf,
      boolean checkVersion)
          throws SentryNoSuchObjectException, SentryAccessDeniedException {
    if (!checkVersion) {
      setSentryVersion(SentryStoreSchemaInfo.getSentryVersion(),
          "Schema version set implicitly");
    } else {
      String currentVersion = getSentryVersion();
      if (!SentryStoreSchemaInfo.getSentryVersion().equals(currentVersion)) {
        throw new SentryAccessDeniedException(
            "The Sentry store schema version " + currentVersion
            + " is different from distribution version "
            + SentryStoreSchemaInfo.getSentryVersion());
      }
    }
  }

  /**
   * PersistenceManager object and Transaction object have a one to one
   * correspondence. Each PersistenceManager object is associated with a
   * transaction object and vice versa. Hence we create a persistence manager
   * instance when we create a new transaction. We create a new transaction
   * for every store API since we want that unit of work to behave as a
   * transaction.
   *
   * Note that there's only one instance of PersistenceManagerFactory object
   * for the service.
   *
   * Synchronized because we obtain persistence manager
   */
  private synchronized PersistenceManager openTransaction() {
    PersistenceManager pm = pmf.getPersistenceManager();
    Transaction currentTransaction = pm.currentTransaction();
    currentTransaction.begin();
    return pm;
  }

  /**
   * Synchronized due to sequence id generation
   */
  private synchronized CommitContext commitUpdateTransaction(PersistenceManager pm) {
    commitTransaction(pm);
    return new CommitContext(SERVER_UUID, incrementGetSequenceId());
  }

  /**
   * Increments commitSequenceId which should not be modified outside
   * this method.
   *
   * @return sequence id
   */
  private synchronized long incrementGetSequenceId() {
    return ++commitSequenceId;
  }

  private void rollbackTransaction(PersistenceManager pm) {
    if (pm == null || pm.isClosed()) {
      return;
    }
    Transaction currentTransaction = pm.currentTransaction();
    if (currentTransaction.isActive()) {
      try {
        currentTransaction.rollback();
      } finally {
        pm.close();
      }
    }
  }

  private void commitTransaction(PersistenceManager pm) {
    Transaction currentTransaction = pm.currentTransaction();
    try {
      Preconditions.checkState(currentTransaction.isActive(), "Transaction is not active");
      currentTransaction.commit();
    } finally {
      pm.close();
    }
  }

  public void setSentryVersion(String newVersion, String verComment)
      throws SentryNoSuchObjectException, SentryAccessDeniedException {
    MSentryVersion mVersion;
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;

    try {
      mVersion = getMSentryVersion();
      if (newVersion.equals(mVersion.getSchemaVersion())) {
        // specified version already in there
        return;
      }
    } catch (SentryNoSuchObjectException e) {
      // if the version doesn't exist, then create it
      mVersion = new MSentryVersion();
    }
    mVersion.setSchemaVersion(newVersion);
    mVersion.setVersionComment(verComment);
    try {
      pm = openTransaction();
      pm.makePersistent(mVersion);
      commitTransaction(pm);
      rollbackTransaction = false;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private MSentryVersion getMSentryVersion()
      throws SentryNoSuchObjectException, SentryAccessDeniedException {
    boolean rollbackTransaction = true;
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      Query query = pm.newQuery(MSentryVersion.class);
      List<MSentryVersion> mSentryVersions = (List<MSentryVersion>) query
          .execute();
      pm.retrieveAll(mSentryVersions);
      commitTransaction(pm);
      rollbackTransaction = false;
      if (mSentryVersions.isEmpty()) {
        throw new SentryNoSuchObjectException("No matching version found");
      }
      if (mSentryVersions.size() > 1) {
        throw new SentryAccessDeniedException(
            "Metastore contains multiple versions");
      }
      return mSentryVersions.get(0);
    } catch (JDODataStoreException e) {
      if (e.getCause() instanceof MissingTableException) {
        throw new SentryAccessDeniedException("Version table not found. "
            + "The sentry store is not set or corrupt ");
      } else {
        throw e;
      }
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  private String getSentryVersion() throws SentryNoSuchObjectException,
  SentryAccessDeniedException {
    MSentryVersion mVersion = getMSentryVersion();
    return mVersion.getSchemaVersion();
  }

  /**
   * close the PersistenceManagerFactory
   */
  @Override
  public void close() {
    if(pmf != null){
      pmf.close();
    }
  }


  @Override
  public CommitContext createSentryRole(String roleName, String requestorUserName)
      throws SentryAlreadyExistsException {
    Preconditions.checkNotNull(roleName);
    Preconditions.checkNotNull(requestorUserName);

    roleName = toTrimedLower(roleName);
    PersistenceManager pm = null;
    boolean rollbackTransaction = true;

    try{
      pm = openTransaction();
      if (null != getRole(roleName, pm)) {
        throw new SentryAlreadyExistsException("roleName:" + roleName + " already exists");
      }
      MSentryRole mRole = new MSentryRole(roleName, System.currentTimeMillis());
      pm.makePersistent(mRole);
      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  protected MSentryRole getRole(String roleName, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    return (MSentryRole)query.execute(roleName);
  }

  protected String getRolesFilter(Set<MSentryRole> roles, Query query) {
    if ((roles == null) || (roles.size() == 0)) {
      return "";
    }
    Set<String> roleNames = Sets.newHashSet();
    for (MSentryRole role : roles) {
      roleNames.add(role.getRoleName());
    }
    return getRoleNamesFilter(roleNames, query);
  }

  protected String getRoleNamesFilter(Set<String> roleNames, Query query) {
    StringBuilder filter = new StringBuilder();
    if ((roleNames != null) && (roleNames.size() > 0)) {
      query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
      List<String> rolesFiler = new LinkedList<String>();
      for (String roleName : roleNames) {
        rolesFiler.add("role.roleName == \"" + roleName.trim().toLowerCase() + "\" ");
      }
      filter.append("roles.contains(role) " + "&& (" + Joiner.on(" || ").join(rolesFiler) + ")");
    }
    return filter.toString();
  }

  @Override
  public CommitContext dropSentryRole(String roleName,
      String requestorUserName) throws SentryNoSuchObjectException {
    Preconditions.checkNotNull(roleName);
    Preconditions.checkNotNull(requestorUserName);

    roleName = toTrimedLower(roleName);
    PersistenceManager pm = null;
    boolean rollbackTransaction = true;

    try{
      pm = openTransaction();
      MSentryRole role = getRole(roleName, pm);
      if (role == null) {
        throw new SentryNoSuchObjectException("roleName:" + roleName + " isn't exist");
      }
      pm.retrieve(role);
      role.removeGMPrivileges();
      pm.deletePersistent(role);
      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  protected void createGroup(String groupName, MSentryRole role, String requestorUserName, PersistenceManager pm) {
    MSentryGroup group = getGroup(groupName, pm);
    if (group == null) {
      group = new MSentryGroup(groupName,
          System.currentTimeMillis(),
          new HashSet<MSentryRole>());
    } else {
      /**
       * retrieve all roles related to this group
       */
      pm.retrieve(group);
    }
    group.appendRole(role);
    pm.makePersistent(group);
  }

  protected MSentryGroup getGroup(String groupName, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryGroup.class);
    query.setFilter("this.groupName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    return (MSentryGroup)query.execute(groupName);
  }

  @Override
  public CommitContext alterSentryRoleAddGroups(String roleName, Set<String> groupNames,
      String requestorUserName) throws SentryNoSuchObjectException {
    Preconditions.checkNotNull(roleName);
    Preconditions.checkNotNull(groupNames);
    Preconditions.checkNotNull(requestorUserName);
    groupNames = toTrimedLower(groupNames);

    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try{
      pm = openTransaction();
      MSentryRole role = getRole(roleName, pm);
      if (role == null) {
        throw new SentryNoSuchObjectException("roleName:" + roleName + " isn't exist");
      }
      for(String groupName : groupNames) {
        createGroup(groupName, role, requestorUserName, pm);
      }
      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleDeleteGroups(String roleName,Set<String> groupNames,
      String requestorUserName) throws SentryNoSuchObjectException {
    Preconditions.checkNotNull(roleName);
    Preconditions.checkNotNull(groupNames);
    Preconditions.checkNotNull(requestorUserName);
    groupNames = toTrimedLower(groupNames);

    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try{
      pm = openTransaction();
      MSentryRole role = getRole(roleName, pm);
      if (role == null) {
        throw new SentryNoSuchObjectException("roleName:" + roleName + " isn't exist");
      }

      List<MSentryGroup> groups = Lists.newArrayList();
      for (String groupName : groupNames) {
        MSentryGroup group = getGroup(groupName, pm);
        if (group == null) {
          throw new SentryNoSuchObjectException("groupName:" + groupName + " isn't exist");
        }
        /**
         * retrieve all roles related to this group
         */
        pm.retrieve(group);

        group.removeRole(role);
        groups.add(group);
      }
      pm.makePersistentAll(groups);
      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public Set<String> getRolesByGroupNames(Set<String> groupNames) {
    Preconditions.checkNotNull(groupNames);
    groupNames = toTrimedLower(groupNames);

    Set<String> roleNames = Sets.newHashSet();
    PersistenceManager pm = null;
    try{
      pm = openTransaction();
      roleNames.addAll(getRoleNamesByGroupNames(groupNames, pm));
      return roleNames;
    } finally {
      commitTransaction(pm);
    }
  }

  private Set<String> getRoleNamesByGroupNames(Set<String> groupNames,
      PersistenceManager pm) {
    Set<String> roleNames = Sets.newHashSet();
    for (String groupName: groupNames) {
      MSentryGroup group = getGroup(groupName, pm);
      if (group != null) {
        /**
         * retrieve all roles related to this group
         */
        pm.retrieve(group);

        for (MSentryRole role: group.getRoles()) {
          roleNames.add(role.getRoleName());
        }
      }
    }
    return roleNames;
  }

  @Override
  public Set<String> getGroupsByRoleNames(Set<String> roleNames){
    Preconditions.checkNotNull(roleNames);
    roleNames = toTrimedLower(roleNames);

    Set<String> groupNames = new HashSet<String>();
    PersistenceManager pm = null;
    try{
      pm = openTransaction();
      groupNames.addAll(getGroupsByRoleNames(roleNames, pm));
      return groupNames;
    } finally {
      commitTransaction(pm);
    }
  }

  private Set<String> getGroupsByRoleNames(Set<String> roleNames, PersistenceManager pm) {
    Set<String> groupNames = Sets.newHashSet();
    for (String roleName : roleNames) {
      MSentryRole role = getRole(roleName, pm);
      if (role != null) {
        /**
         * retrieve all groups related to this role
         */
        pm.retrieve(role);

        for (MSentryGroup group: role.getGroups()) {
          groupNames.add(group.getGroupName());
        }
      }
    }
    return groupNames;
  }

  @Override
  public Set<PrivilegeObject> getPrivilegesByRoleName(Set<String> roleNames) {
    Preconditions.checkNotNull(roleNames);
    roleNames = toTrimedLower(roleNames);

    Set<PrivilegeObject> privileges = Sets.newHashSet();
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      privileges.addAll(getPrivilegesByRole(roleNames, pm));
    } finally {
      commitTransaction(pm);
    }
    return privileges;
  }

  @Override
  public Set<PrivilegeObject> getPrivilegesByProvider(String component,
      String service, Set<String> roleNames, Set<String> groupNames,
      List<? extends Authorizable> authorizables) throws SentryUserException {
    Preconditions.checkNotNull(component);
    Preconditions.checkNotNull(service);
    component = toTrimedLower(component);
    service = toTrimedLower(service);

    Set<PrivilegeObject> privileges = Sets.newHashSet();
    PersistenceManager pm = null;
    try {
      pm = openTransaction();
      //CaseInsensitive names
      roleNames = (roleNames != null ? toTrimedLower(roleNames) : new HashSet<String>());
      groupNames = (groupNames != null ? toTrimedLower(groupNames) : new HashSet<String>());

      roleNames.addAll(getRoleNamesByGroupNames(groupNames, pm));
      //get the privileges
      privileges.addAll(getPrivilegesByProvider(component, service, roleNames, authorizables, pm));
    } finally {
      commitTransaction(pm);
    }
    return privileges;
  }

  @Override
  public CommitContext alterSentryRoleGrantPrivilege(String roleName, PrivilegeObject privilege,
      String grantorPrincipal) throws SentryUserException {
    Preconditions.checkNotNull(roleName);
    Preconditions.checkNotNull(grantorPrincipal);

    roleName = toTrimedLower(roleName);
    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try{
      pm = openTransaction();
      MSentryRole role = getRole(roleName, pm);
      if (role == null) {
        throw new SentryNoSuchObjectException("roleName:" + roleName + " isn't exist");
      }
      /**
       * get all privileges related to this role
       */
      pm.retrieve(role);
      /**
       * check with grant option
       */
      grantOptionCheck(privilege, grantorPrincipal, role,
          getRequestorGroups(grantorPrincipal), pm);

      grantPrivilege(privilege, grantorPrincipal, role, pm);
      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;

    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public CommitContext alterSentryRoleRevokePrivilege(String roleName, PrivilegeObject privilege,
      String grantorPrincipal) throws SentryUserException {
    Preconditions.checkNotNull(roleName);
    Preconditions.checkNotNull(grantorPrincipal);

    roleName = toTrimedLower(roleName);
    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try{
      pm = openTransaction();
      MSentryRole role = getRole(roleName, pm);
      if (role == null) {
        throw new SentryNoSuchObjectException("roleName:" + roleName + " isn't exist");
      }
      /**
       * get all privileges related to this role
       */
      pm.retrieve(role);
      //check with grant option
      grantOptionCheck(privilege, grantorPrincipal, role,
          getRequestorGroups(grantorPrincipal), pm);

      revokePrivilege(privilege, grantorPrincipal, role, pm);
      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;

    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public CommitContext dropSentryPrivilege(PrivilegeObject privilege,
      String requestorUserName) throws SentryUserException {
    Preconditions.checkNotNull(requestorUserName);

    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try {
      pm = openTransaction();
      dropPrivilege(privilege, requestorUserName, pm);
      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  @Override
  public CommitContext renameSentryPrivilege(String component, String service,
      List<? extends Authorizable> oldAuthorizables, List<? extends Authorizable> newAuthorizables,
      String requestorUserName) throws SentryUserException {
    Preconditions.checkNotNull(component);
    Preconditions.checkNotNull(service);
    Preconditions.checkNotNull(oldAuthorizables);
    Preconditions.checkNotNull(newAuthorizables);

    if (oldAuthorizables.size() != newAuthorizables.size()) {
      throw new SentryAccessDeniedException(
          "rename privilege denied: the size of oldAuthorizables must equals the newAuthorizables "
              + "oldAuthorizables:" + Arrays.toString(oldAuthorizables.toArray()) + " "
              + "newAuthorizables:" + Arrays.toString(newAuthorizables.toArray()));
    }

    PersistenceManager pm = null;
    boolean rollbackTransaction = true;
    try {
      pm = openTransaction();
      renamePrivilege(toTrimedLower(component), toTrimedLower(service), oldAuthorizables, newAuthorizables, requestorUserName, pm);
      CommitContext commitContext = commitUpdateTransaction(pm);
      rollbackTransaction = false;
      return commitContext;
    } finally {
      if (rollbackTransaction) {
        rollbackTransaction(pm);
      }
    }
  }

  /**
   * Grant option check
   * @param pm
   * @param privilegeReader
   * @param role
   * @throws SentryUserException
   */
  private void grantOptionCheck(PrivilegeObject requestPrivilege, String grantorPrincipal,
      MSentryRole role, Set<String> grantorGroups, PersistenceManager pm)
      throws SentryUserException {

    if (Strings.isNullOrEmpty(grantorPrincipal)) {
      throw new SentryInvalidInputException("grantorPrincipal should not be null or empty");
    }

    grantorGroups = (grantorGroups != null ? toTrimedLower(grantorGroups) : new HashSet<String>());
    //admin group check
    if (!Sets.intersection(adminGroups, grantorGroups).isEmpty()) {
      return;
    }

    Set<String> roleNames = getRoleNamesByGroupNames(grantorGroups, pm);
    Set<PrivilegeObject> privileges = getPrivilegesByRole(roleNames, pm);

    //privilege grant option check
    boolean hasGrant = false;
    for (PrivilegeObject privilege : privileges) {
      if (privilege.getGrantOption() && PrivilegeUtil.implies(privilege, requestPrivilege)) {
        hasGrant = true;
        break;
      }
    }
    if (!hasGrant) {
      throw new SentryGrantDeniedException(grantorPrincipal
          + " has no grant!");
    }
  }

  public static Action getAction(String component, String value) {
    ActionReader actionReader = actionReaders.get(component.toLowerCase());
    if (actionReader == null) {
      throw new RuntimeException("can't get actionReader for component:" + component);
    }
    return actionReader.deserialize(value);
  }

  protected abstract void grantPrivilege(PrivilegeObject privilege,
      String grantorPrincipal, MSentryRole role, PersistenceManager pm)
          throws SentryUserException;

  protected abstract void revokePrivilege(PrivilegeObject privilege,
      String grantorPrincipal, MSentryRole role, PersistenceManager pm)
          throws SentryUserException;

  protected abstract void dropPrivilege(PrivilegeObject privilege,
      String grantorPrincipal, PersistenceManager pm) throws SentryUserException;

  protected abstract void renamePrivilege(String component, String service,
      List<? extends Authorizable> oldAuthorizables, List<? extends Authorizable> newAuthorizables,
      String grantorPrincipal, PersistenceManager pm) throws SentryUserException;

  protected abstract Set<PrivilegeObject> getPrivilegesByRole(Set<String> roleNames,
      PersistenceManager pm);

  protected abstract Set<PrivilegeObject> getPrivilegesByProvider(String component, String service,
      Set<String> roleNames, List<? extends Authorizable> authorizables,
      PersistenceManager pm);

}
