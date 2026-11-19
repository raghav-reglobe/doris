// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.SuUserCommand;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * hide_inaccessible_columns in a session switched by SU: the user holds the property, the user's own session is
 * narrowed, and a session that another account switched to the user keeps the refusal.
 */
public class HideInaccessibleColumnsSwitchedSessionTest extends TestWithFeService {
    private static final String PERSON = "hic_person";
    private static final String DENIED = "does not have privilege";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("hics");
        connectContext.setDatabase("hics");
        createTable("create table hics.t (c_id bigint, c_name varchar(20), c_secret varchar(20))"
                + " duplicate key(c_id) distributed by hash(c_id) buckets 1 properties('replication_num' = '1')");
        createRole("hics_role");
        addUser(PERSON, true);
        grantPriv("GRANT SELECT_PRIV(c_id, c_name) ON internal.hics.t TO ROLE 'hics_role'");
        grantRole("GRANT 'hics_role' TO '" + PERSON + "'@'%'");
        Env.getCurrentEnv().getAuth().updateUserPropertyInternal(PERSON,
                Lists.newArrayList(Pair.of(UserProperty.PROP_HIDE_INACCESSIBLE_COLUMNS, "true")), false);
    }

    /**
     * Runs the statement the way a connection does: parsed first, then executed. The star is narrowed only in a
     * statement whose parsed form the statement context holds, which the executor's test constructor leaves out.
     * Returns the plan, or the message the statement ends with.
     */
    private String planOrMessage(String sql) throws Exception {
        connectContext.setThreadLocalInfo();
        connectContext.getState().reset();
        StatementBase statement = new NereidsParser().parseSQL(sql, connectContext.getSessionVariable()).get(0);
        statement.setOrigStmt(new OriginStatement(sql, 0));
        statement.setUserInfo(connectContext.getCurrentUserIdentity());
        StmtExecutor executor = new StmtExecutor(connectContext, statement);
        connectContext.setExecutor(executor);
        executor.execute();
        if (connectContext.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            return connectContext.getState().getErrorMessage();
        }
        return executor.planner().getExplainString(new ExplainOptions(false, false, false));
    }

    @Test
    public void testSwitchedSessionKeepsTheRefusal() throws Exception {
        UserIdentity person = UserIdentity.createAnalyzedUserIdentWithIp(PERSON, "%");

        // the person's own session
        connectContext.setCurrentUserIdentity(person);
        try {
            String plan = planOrMessage("explain select * from hics.t");
            Assertions.assertFalse(plan.contains(DENIED), plan);
            int note = plan.indexOf("hide_inaccessible_columns: the star left out");
            Assertions.assertTrue(note >= 0, plan);
            Assertions.assertFalse(plan.substring(0, note).contains("c_secret"), plan);
            Assertions.assertTrue(plan.substring(0, note).contains("c_name"), plan);
        } finally {
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }

        // a session switched to the person, with the same role
        new SuUserCommand(person, Collections.singletonList("hics_role"), null).run(connectContext, null);
        try {
            Assertions.assertEquals(PERSON, connectContext.getQualifiedUser());
            String message = planOrMessage("explain select * from hics.t");
            Assertions.assertTrue(message.contains(DENIED), message);
            String plan = planOrMessage("explain select c_id, c_name from hics.t");
            Assertions.assertFalse(plan.contains(DENIED), plan);
        } finally {
            connectContext.revertSessionNarrowing();
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }
    }
}
