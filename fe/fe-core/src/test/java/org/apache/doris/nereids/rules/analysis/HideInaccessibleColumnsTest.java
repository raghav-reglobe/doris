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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.SetUserPropertyVarOp;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The user property hide_inaccessible_columns. Two users hold the same grants: every column of hic.t but c_secret,
 * the columns c_id and c_name of the view hic.v, and the whole of hic.d, hic.v_safe and hic.v_none. HIDER has the
 * property set, PLAIN has not.
 */
public class HideInaccessibleColumnsTest extends TestWithFeService {
    private static final String HIDER = "hic_hider";
    private static final String PLAIN = "hic_plain";
    private static final String DENIED = "does not have privilege";
    private static final String NOTE = "hide_inaccessible_columns: the star left out";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("hic");
        connectContext.setDatabase("hic");
        String rest = " distributed by hash(c_id) buckets 1 properties('replication_num' = '1')";
        createTables(
                "create table hic.t (c_id bigint, c_name varchar(20), c_secret varchar(20), c_amount int)"
                        + " duplicate key(c_id)" + rest,
                "create table hic.d (c_id bigint, c_label varchar(20)) duplicate key(c_id)" + rest,
                "create table hic.none (c_id bigint, c_x varchar(20)) duplicate key(c_id)" + rest,
                "create table hic.sink (c_id bigint, c_name varchar(20), c_secret varchar(20), c_amount int)"
                        + " duplicate key(c_id)" + rest);
        createView("create view hic.v as select c_id, c_name, c_secret from hic.t");
        createView("create view hic.v_safe as select c_id, c_name from hic.t");
        createView("create view hic.v_none as select * from hic.none");
        for (String user : new String[] {HIDER, PLAIN}) {
            addUser(user, true);
            grantPriv("GRANT SELECT_PRIV(c_id, c_name, c_amount) ON internal.hic.t TO '" + user + "'");
            grantPriv("GRANT SELECT_PRIV ON internal.hic.d TO '" + user + "'");
            grantPriv("GRANT SELECT_PRIV(c_id, c_name) ON internal.hic.v TO '" + user + "'");
            grantPriv("GRANT SELECT_PRIV ON internal.hic.v_safe TO '" + user + "'");
            grantPriv("GRANT SELECT_PRIV ON internal.hic.v_none TO '" + user + "'");
            grantPriv("GRANT LOAD_PRIV ON internal.hic.sink TO '" + user + "'");
        }
        Env.getCurrentEnv().getAuth().updateUserPropertyInternal(HIDER,
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

    /** The plan of the statement for the user, or the message the statement ends with. */
    private String explainAs(String user, String sql) throws Exception {
        useUser(user);
        try {
            return planOrMessage("explain " + sql);
        } finally {
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }
    }

    /** The star of the statement leaves c_secret out, keeps the given columns, and EXPLAIN says what it left out. */
    private void assertNarrowed(String sql, String... kept) throws Exception {
        String plan = explainAs(HIDER, sql);
        Assertions.assertFalse(plan.contains(DENIED), sql + "\n" + plan);
        int note = plan.indexOf(NOTE);
        Assertions.assertTrue(note >= 0, sql + " should say what the star left out\n" + plan);
        Assertions.assertTrue(plan.substring(note).contains("c_secret"), sql + "\n" + plan);
        String body = plan.substring(0, note);
        Assertions.assertFalse(body.contains("c_secret"), sql + "\n" + plan);
        for (String column : kept) {
            Assertions.assertTrue(body.contains(column), sql + " should keep " + column + "\n" + plan);
        }
    }

    private void assertRefused(String user, String sql) throws Exception {
        String message = explainAs(user, sql);
        Assertions.assertTrue(message.contains(DENIED), user + ": " + sql + "\n" + message);
    }

    @Test
    public void testStarIsRefusedWithoutTheProperty() throws Exception {
        assertRefused(PLAIN, "select * from hic.t");
        assertRefused(PLAIN, "select t.* from hic.t t join hic.d d on t.c_id = d.c_id");
        assertRefused(PLAIN, "select * from hic.v");
        assertRefused(PLAIN, "select * from (select * from hic.t) q");
        String plan = explainAs(PLAIN, "select c_id, c_name, c_amount from hic.t");
        Assertions.assertFalse(plan.contains(DENIED), plan);
    }

    @Test
    public void testStarOverTablesAndViewsIsNarrowed() throws Exception {
        assertNarrowed("select * from hic.t", "c_id", "c_name", "c_amount");
        assertNarrowed("select * from hic.t where c_id > 0", "c_id", "c_name", "c_amount");
        assertNarrowed("select t.* from hic.t t", "c_id", "c_name", "c_amount");
        assertNarrowed("select * from hic.t t join hic.d d on t.c_id = d.c_id", "c_amount", "c_label");
        assertNarrowed("select t.*, d.c_label from hic.t t join hic.d d on t.c_id = d.c_id", "c_amount", "c_label");
        assertNarrowed("select * except(c_amount) from hic.t", "c_id", "c_name");
        // the list a user writes today to get past the refusal keeps working, with and without the property
        assertNarrowed("select * except(c_secret) from hic.t", "c_id", "c_name", "c_amount");
        assertNarrowed("select t.* except(c_secret), d.c_label from hic.t t join hic.d d on t.c_id = d.c_id",
                "c_amount", "c_label");
        assertNarrowed("select * except(c_secret, c_amount) from hic.t", "c_id", "c_name");
        String plain = explainAs(PLAIN, "select * except(c_secret) from hic.t");
        Assertions.assertFalse(plain.contains(DENIED), plain);
        Assertions.assertFalse(plain.contains(NOTE), plain);
        // the view's own column grants decide for a view
        assertNarrowed("select * from hic.v", "c_id", "c_name");
    }

    @Test
    public void testStarInsideSubqueryIsNarrowedWhereItIsBound() throws Exception {
        assertNarrowed("select * from (select * from hic.t) q", "c_id", "c_name", "c_amount");
        assertNarrowed("with c as (select * from hic.t) select * from c", "c_id", "c_name", "c_amount");
        assertNarrowed("select * from hic.t union all select * from hic.t", "c_id", "c_name", "c_amount");
    }

    @Test
    public void testStatementWithoutParsedFormIsNotNarrowed() throws Exception {
        // e.g. a statement the FE runs for itself: without the parsed statement nothing says its rows go to a client
        useUser(HIDER);
        try {
            String message = getSQLPlanOrErrorMsg("explain select * from hic.t");
            Assertions.assertTrue(message.contains(DENIED), message);
        } finally {
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }
    }

    @Test
    public void testNamedColumnIsStillRefused() throws Exception {
        assertRefused(HIDER, "select c_secret from hic.t");
        assertRefused(HIDER, "select c_id from hic.t where c_secret = 'x'");
        assertRefused(HIDER, "select c_secret from hic.v");
        // the star does not drop a column the statement itself names below it
        assertRefused(HIDER, "select * from (select c_id, c_secret from hic.t) q");
        assertRefused(HIDER, "with c as (select c_id, c_secret from hic.t) select * from c");
    }

    @Test
    public void testTableWithNoReadableColumnIsRefusedAsBefore() throws Exception {
        assertRefused(HIDER, "select * from hic.none");
        assertRefused(PLAIN, "select * from hic.none");
    }

    @Test
    public void testViewBodyIsNotNarrowed() throws Exception {
        // the reader holds the view and nothing of the table under it: the star in the view's body stays whole
        for (String user : new String[] {HIDER, PLAIN}) {
            String plan = explainAs(user, "select * from hic.v_none");
            Assertions.assertFalse(plan.contains(DENIED), user + "\n" + plan);
            Assertions.assertTrue(plan.contains("c_x"), user + "\n" + plan);
            Assertions.assertFalse(plan.contains(NOTE), user + "\n" + plan);
            plan = explainAs(user, "select * from hic.v_safe");
            Assertions.assertFalse(plan.contains(DENIED), user + "\n" + plan);
            Assertions.assertTrue(plan.contains("c_name"), user + "\n" + plan);
        }
    }

    @Test
    public void testStatementThatStoresItsRowsKeepsTheRefusal() throws Exception {
        // narrowed, the column list of the INSERT would shift by position
        assertRefused(HIDER, "insert into hic.sink select * from hic.t");
    }

    @Test
    public void testPropertyIsBooleanShownAndAdminOnly() throws Exception {
        UserProperty property = new UserProperty();
        Assertions.assertFalse(property.isHideInaccessibleColumns());
        property.update(Lists.newArrayList(Pair.of("HIDE_inaccessible_columns", "TRUE")));
        Assertions.assertTrue(property.isHideInaccessibleColumns());
        Assertions.assertTrue(property.fetchProperty().stream().anyMatch(row ->
                UserProperty.PROP_HIDE_INACCESSIBLE_COLUMNS.equals(row.get(0)) && "true".equals(row.get(1))));
        Assertions.assertThrows(DdlException.class, () ->
                property.update(Lists.newArrayList(Pair.of(UserProperty.PROP_HIDE_INACCESSIBLE_COLUMNS, "yes"))));
        Assertions.assertTrue(property.isHideInaccessibleColumns());
        property.update(Lists.newArrayList(Pair.of(UserProperty.PROP_HIDE_INACCESSIBLE_COLUMNS, "false")));
        Assertions.assertFalse(property.isHideInaccessibleColumns());

        // a user does not set it for themselves
        ConnectContext own = new ConnectContext();
        own.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(PLAIN, "%"));
        own.setThreadLocalInfo();
        try {
            Assertions.assertThrows(UserException.class, () -> new SetUserPropertyVarOp(PLAIN,
                    UserProperty.PROP_HIDE_INACCESSIBLE_COLUMNS, "true").validate(own));
        } finally {
            connectContext.setThreadLocalInfo();
        }
        new SetUserPropertyVarOp(PLAIN, UserProperty.PROP_HIDE_INACCESSIBLE_COLUMNS, "false").validate(connectContext);
    }
}
