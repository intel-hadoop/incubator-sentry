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
package org.apache.sentry.binding.hive.v2.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.SentryHiveConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.exec.SentryHivePrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.authorization.AuthorizationParseUtils;
import org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactoryImpl;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.GrantRevokeRoleDDL;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.plan.RoleDDLDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.core.model.db.AccessConstants;

public class SentryAuthorizationTaskFactoryImplV2 extends HiveAuthorizationTaskFactoryImpl {
  private final HiveConf conf;

  public SentryAuthorizationTaskFactoryImplV2(HiveConf conf, Hive db) {
    super(conf, db);
    this.conf = conf;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Task<? extends Serializable> createCreateRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    String roleName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
    if (AccessConstants.RESERVED_ROLE_NAMES.contains(roleName.toUpperCase())) {
      String msg = "Roles cannot be one of the reserved roles: " + AccessConstants.RESERVED_ROLE_NAMES;
      throw new RuntimeException(msg);
    }
    RoleDDLDesc roleDesc = new RoleDDLDesc(roleName, RoleDDLDesc.RoleOperation.CREATE_ROLE);
    return TaskFactory.get(new DDLWork(inputs, outputs, roleDesc), conf);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Task<? extends Serializable> createDropRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    String roleName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
    if (AccessConstants.RESERVED_ROLE_NAMES.contains(roleName.toUpperCase())) {
      String msg = "Roles cannot be one of the reserved roles: " + AccessConstants.RESERVED_ROLE_NAMES;
      throw new RuntimeException(msg);
    }
    RoleDDLDesc roleDesc = new RoleDDLDesc(roleName, RoleDDLDesc.RoleOperation.DROP_ROLE);
    return TaskFactory.get(new DDLWork(inputs, outputs, roleDesc), conf);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Task<? extends Serializable> createShowRoleGrantTask(ASTNode ast, Path resultFile,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) {
    ASTNode child = (ASTNode) ast.getChild(0);
    PrincipalType principalType = PrincipalType.USER;
    switch (child.getType()) {
    case HiveParser.TOK_USER:
      principalType = PrincipalType.USER;
      break;
    case HiveParser.TOK_GROUP:
      principalType = PrincipalType.GROUP;
      break;
    case HiveParser.TOK_ROLE:
      principalType = PrincipalType.ROLE;
      break;
    }
    if (principalType != PrincipalType.GROUP) {
      String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principalType;
      throw new RuntimeException(msg);
    }
    String principalName = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
    RoleDDLDesc roleDesc = new RoleDDLDesc(principalName, principalType,
        RoleDDLDesc.RoleOperation.SHOW_ROLE_GRANT, null);
    roleDesc.setResFile(resultFile.toString());
    return TaskFactory.get(new DDLWork(inputs, outputs,  roleDesc), conf);
  }

  @Override
  public Task<? extends Serializable> createGrantTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    List<PrincipalDesc> principalDesc = AuthorizationParseUtils.analyzePrincipalListDef(
        (ASTNode) ast.getChild(1));
    for (PrincipalDesc princ : principalDesc) {
      if (princ.getType() != PrincipalType.ROLE) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + princ.getType();
        throw new SemanticException(msg);
      }
    }

    return super.createGrantTask(ast, inputs, outputs);
  }

  @Override
  public Task<? extends Serializable> createRevokeTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {
    List<PrincipalDesc> principalDesc = AuthorizationParseUtils.analyzePrincipalListDef(
        (ASTNode) ast.getChild(1));
    for (PrincipalDesc princ : principalDesc) {
      if (princ.getType() != PrincipalType.ROLE) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + princ.getType();
        throw new SemanticException(msg);
      }
    }

    return super.createRevokeTask(ast, inputs, outputs);
  }

  @Override
  public Task<? extends Serializable> createGrantRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    return analyzeGrantRevokeRole(true, ast, inputs, outputs);
  }

  @Override
  public Task<? extends Serializable> createShowGrantTask(ASTNode ast, Path resultFile, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) throws SemanticException {

    ASTNode principal = (ASTNode) ast.getChild(0);
    PrincipalType type = PrincipalType.USER;
    switch (principal.getType()) {
    case HiveParser.TOK_USER:
      type = PrincipalType.USER;
      break;
    case HiveParser.TOK_GROUP:
      type = PrincipalType.GROUP;
      break;
    case HiveParser.TOK_ROLE:
      type = PrincipalType.ROLE;
      break;
    }
    if (type != PrincipalType.ROLE) {
      String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + type;
      throw new SemanticException(msg);
    }

    return super.createShowGrantTask(ast, resultFile, inputs, outputs);
  }

  @Override
  public Task<? extends Serializable> createRevokeRoleTask(ASTNode ast, HashSet<ReadEntity> inputs,
      HashSet<WriteEntity> outputs) {
    return analyzeGrantRevokeRole(false, ast, inputs, outputs);
  }

  @SuppressWarnings("unchecked")
  private Task<? extends Serializable> analyzeGrantRevokeRole(boolean isGrant, ASTNode ast,
      HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) {
    List<PrincipalDesc> principalDesc = AuthorizationParseUtils.analyzePrincipalListDef(
        (ASTNode) ast.getChild(0));
    for (PrincipalDesc princ : principalDesc) {
      if (princ.getType() != PrincipalType.GROUP) {
        String msg = SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_ON_OBJECT + princ.getType();
        throw new RuntimeException(msg);
      }
    }

    //check if admin option has been specified
    int rolesStartPos = 1;
    ASTNode wAdminOption = (ASTNode) ast.getChild(1);
    boolean isAdmin = false;
    if((isGrant && wAdminOption.getToken().getType() == HiveParser.TOK_GRANT_WITH_ADMIN_OPTION) ||
       (!isGrant && wAdminOption.getToken().getType() == HiveParser.TOK_ADMIN_OPTION_FOR)){
      rolesStartPos = 2; //start reading role names from next position
      isAdmin = true;
    }

    List<String> roles = new ArrayList<String>();
    for (int i = rolesStartPos; i < ast.getChildCount(); i++) {
      roles.add(BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(i).getText()));
    }

    String roleOwnerName = SessionState.getUserFromAuthenticator();

    //until change is made to use the admin option. Default to false with V2 authorization

    GrantRevokeRoleDDL grantRevokeRoleDDL = new GrantRevokeRoleDDL(isGrant,
        roles, principalDesc, roleOwnerName, PrincipalType.USER, isAdmin);
    return TaskFactory.get(new DDLWork(inputs, outputs, grantRevokeRoleDDL), conf);
  }

  protected PrivilegeObjectDesc parsePrivObject(ASTNode ast) throws SemanticException {
    SentryHivePrivilegeObjectDesc subject = new SentryHivePrivilegeObjectDesc();
    ASTNode child = (ASTNode) ast.getChild(0);
    ASTNode gchild = (ASTNode)child.getChild(0);
    if (child.getType() == HiveParser.TOK_TABLE_TYPE) {
      subject.setTable(true);
      String[] qualified = BaseSemanticAnalyzer.getQualifiedTableName(gchild);
      subject.setObject(BaseSemanticAnalyzer.getDotName(qualified));
    } else if (child.getType() == HiveParser.TOK_URI_TYPE) {
      subject.setUri(true);
      subject.setObject(gchild.getText());
    } else if (child.getType() == HiveParser.TOK_SERVER_TYPE) {
      subject.setServer(true);
      subject.setObject(gchild.getText());
    } else {
      subject.setTable(false);
      subject.setObject(BaseSemanticAnalyzer.unescapeIdentifier(gchild.getText()));
    }
    //if partition spec node is present, set partition spec
    for (int i = 1; i < child.getChildCount(); i++) {
      gchild = (ASTNode) child.getChild(i);
      if (gchild.getType() == HiveParser.TOK_PARTSPEC) {
        subject.setPartSpec(DDLSemanticAnalyzer.getPartSpec(gchild));
      } else if (gchild.getType() == HiveParser.TOK_TABCOLNAME) {
        subject.setColumns(BaseSemanticAnalyzer.getColumnNames(gchild));
      }
    }
    return subject;
  }
}
