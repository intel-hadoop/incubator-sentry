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

import java.util.Iterator;

import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.AccessConstants;

import static org.apache.sentry.provider.db.genericModel.service.persistent.SentryStoreBase.getAction;

public class PrivilegeUtil {

  /**
   * Return true if this privilege implies the request privilege
   * Otherwise, return false
   * @param privileges
   * @param requestPrivilege
   * @return boolean
   */
  public static boolean implies(PrivilegeObject privilege,
      PrivilegeObject requestPrivilege) {
    if (!privilege.getComponent().equalsIgnoreCase(
        requestPrivilege.getComponent())) {
      return false;
    }
    if (!privilege.getService().equalsIgnoreCase(
        requestPrivilege.getService())) {
      return false;
    }
    //action check
    Action allAction = getAction(privilege.getComponent(), Action.ALL);
    Action existAction = getAction(privilege.getComponent(), privilege.getAction());
    Action requestAction = getAction(privilege.getComponent(), requestPrivilege.getAction());

    if (!existAction.equals(allAction)) {
      if (requestAction.equals(allAction)) {
        return false;
      }
      if (!existAction.equals(requestAction)) {
        return false;
      }
    }
    //authorizables check
    Iterator<? extends Authorizable> existIterator = privilege.getAuthorizables().iterator();
    Iterator<? extends Authorizable> requestIterator = requestPrivilege.getAuthorizables().iterator();
    while (existIterator.hasNext() && requestIterator.hasNext()) {
      Authorizable existAuthorizable = existIterator.next();
      Authorizable requestAuthorizable = requestIterator.next();
      //check authorizable type
      if (!existAuthorizable.getTypeName().equalsIgnoreCase(
          requestAuthorizable.getTypeName())) {
        return false;
      }
      //check authorizable name
      if (!existAuthorizable.getName().equalsIgnoreCase(
          requestAuthorizable.getName())) {
        /**The persistent authorizable isn't equal the request authorizable
        * but the following situations are pass check
        * 1.The name of persistent authorizable is ALL or "*"
        * 2.The name of requested authorizable is ALL or "*"
        */
        if (existAuthorizable.getName().equalsIgnoreCase("ALL")
            || existAuthorizable.getName().equalsIgnoreCase(AccessConstants.ALL)
            || requestAuthorizable.getName().equalsIgnoreCase("ALL")
            || requestAuthorizable.getName().equalsIgnoreCase(AccessConstants.ALL)) {
          continue;
        } else {
          return false;
        }
      }
    }

    if ( (!existIterator.hasNext()) && (!requestIterator.hasNext()) ){
      /**
       * The persistent privilege has the same authorizables as the requested privilege
       * The check is pass
       */
      return true;

    } else if (existIterator.hasNext()) {
      /**
       * The persistent privilege has much more authorizables than request privilege,so its scope is less
       * than the requested privilege.
       * There is a situation that the check is pass, the name of the exceeding authorizables is ALL or "*"
       */
      while (existIterator.hasNext()) {
        Authorizable existAuthorizable = existIterator.next();
        if (existAuthorizable.getName().equalsIgnoreCase(AccessConstants.ALL)
            || existAuthorizable.getName().equalsIgnoreCase("ALL")) {
          continue;
        } else {
          return false;
        }
      }
    } else {
      /**
       * The requested privilege has much more authorizables than persistent privilege, so its scope is less
       * than the persistent privilege
       * The check is pass
       */
      return true;
    }
    return true;
  }
}
