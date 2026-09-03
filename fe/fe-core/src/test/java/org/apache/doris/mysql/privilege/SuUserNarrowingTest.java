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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.SuUserCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

/**
 * SU (session-narrowed identity switch) acceptance tests:
 * (a) a narrowed session loses the target's personal grants (override REPLACES the role union);
 * (c) re-SU is refused; (d) a session that skips SU keeps only the switcher's own grants;
 * (f) narrowing never leaks into checks of OTHER identities; plus the dormant (SU-only) role
 * gating, both ceiling modes, the config gate, and the parser round trip.
 * End-to-end row-policy + audit coverage runs on the scratch rig (gate battery), not here.
 */
public class SuUserNarrowingTest extends TestWithFeService {

    private static final String CTL = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Override
    protected void runBeforeAll() throws Exception {
        // GRANT validates database existence on the internal catalog
        createDatabase("test");
        createDatabase("perso");
        createDatabase("perso2");
    }

    private UserIdentity ident(String name) {
        return UserIdentity.createAnalyzedUserIdentWithIp(name, "%");
    }

    private boolean canSelectDb(UserIdentity user, String db) {
        return Env.getCurrentEnv().getAuth().checkDbPriv(user, CTL, db, PrivPredicate.SELECT);
    }

    @Test
    public void testOverrideReplacesRoleUnionAndDoesNotLeak() throws Exception {
        addUser("alice", true);
        addUser("bystander", true);
        createRole("space_a");
        createRole("bystander_r");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'space_a';");
        grantRole("GRANT 'space_a' TO 'alice'@'%'");
        grantRole("GRANT 'bystander_r' TO 'bystander'@'%'");
        // personal (default-role) grant on another db
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'alice'@'%';");

        UserIdentity alice = ident("alice");
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(alice);
        ctx.setThreadLocalInfo();
        try {
            // un-narrowed: personal + role grants both live
            Assert.assertTrue(canSelectDb(alice, "perso"));
            Assert.assertTrue(canSelectDb(alice, "test"));

            ctx.setSessionRoleOverride(Collections.singleton("space_a"));
            // (a) the override REPLACES everything: the personal grant is gone...
            Assert.assertFalse(canSelectDb(alice, "perso"));
            // ...while the requested role's grants remain
            Assert.assertTrue(canSelectDb(alice, "test"));

            // (f) checks against OTHER identities are untouched by this session's narrowing
            Set<String> bystanderRoles = Env.getCurrentEnv().getAuth()
                    .getRoleNamesByUserWithLdap(ident("bystander"), false);
            Assert.assertTrue(bystanderRoles.contains("bystander_r"));

            // revert restores the full union
            ctx.setSessionRoleOverride(null);
            Assert.assertTrue(canSelectDb(alice, "perso"));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testDormantRolesInertUntilRequested() throws Exception {
        addUser("dorm_user", true);
        createRole("space_dorm");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'space_dorm';");
        grantRole("GRANT 'space_dorm' TO 'dorm_user'@'%'");
        grantPriv("GRANT SELECT_PRIV ON internal.perso2.* TO 'dorm_user'@'%';");

        UserIdentity user = ident("dorm_user");
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(user);
        ctx.setThreadLocalInfo();
        String savedPattern = Config.su_only_roles_pattern;
        try {
            Config.su_only_roles_pattern = "^space_";
            // dormant in a normal session: the role's grants are inert, personal grants live
            Assert.assertFalse(canSelectDb(user, "test"));
            Assert.assertTrue(canSelectDb(user, "perso2"));
            // the RAW view still carries it (the SU ceiling and admin introspection see it)
            Assert.assertTrue(Env.getCurrentEnv().getAuth()
                    .getGrantedRoleNamesRaw(user).contains("space_dorm"));
            // explicit activation via the override path
            ctx.setSessionRoleOverride(Collections.singleton("space_dorm"));
            Assert.assertTrue(canSelectDb(user, "test"));
            // invalid pattern degrades to disabled (logged), never to an exception
            ctx.setSessionRoleOverride(null);
            Config.su_only_roles_pattern = "([bad";
            Assert.assertTrue(canSelectDb(user, "test"));
        } finally {
            Config.su_only_roles_pattern = savedPattern;
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testSuCommandContract() throws Exception {
        addUser("svc", true);
        addUser("bobby", true);
        createRole("r_bobby");
        createRole("r_ungranted");
        createRole("r_svc_only");
        grantRole("GRANT 'r_bobby' TO 'bobby'@'%'");
        grantRole("GRANT 'r_svc_only' TO 'svc'@'%'");

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(ident("svc"));
        ctx.setThreadLocalInfo();
        String savedUsers = Config.switch_user_users;
        String savedCeiling = Config.switch_user_role_ceiling;
        try {
            // gate: neither ADMIN nor listed -> refused; (d) the session stays the service account
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("bobby", "%"),
                            Collections.singletonList("r_bobby"), null).run(ctx, null));
            Assert.assertNull(ctx.getAuthenticatedIdentity());
            Assert.assertFalse(canSelectDb(ident("svc"), "test"));

            Config.switch_user_users = ctx.getQualifiedUser();

            // nonexistent target refused
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("ghost", "%"),
                            Collections.singletonList("r_bobby"), null).run(ctx, null));

            // NARROWING-ONLY LAW (default ceiling = target): role exists but is not bobby's
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("bobby", "%"),
                            Collections.singletonList("r_ungranted"), null).run(ctx, null));

            // legal switch
            new SuUserCommand(new UserIdentity("bobby", "%"),
                    Collections.singletonList("r_bobby"), "wg_probe").run(ctx, null);
            Assert.assertEquals("bobby", ctx.getCurrentUserIdentity().getQualifiedUser());
            Assert.assertEquals(Collections.singleton("r_bobby"), ctx.getSessionRoleOverride());
            Assert.assertEquals("svc", ctx.getAuthenticatedIdentity().getQualifiedUser());
            Assert.assertEquals("wg_probe", ctx.getSessionVariable().getWorkloadGroup());

            // (c) re-SU refused in a switched session
            Assert.assertThrows(AnalysisException.class, () ->
                    new SuUserCommand(new UserIdentity("svc", "%"),
                            Collections.singletonList("r_svc_only"), null).run(ctx, null));

            // reset reverts to the AUTHENTICATED identity, never widens the target
            ctx.revertSessionNarrowing();
            Assert.assertEquals("svc", ctx.getCurrentUserIdentity().getQualifiedUser());
            Assert.assertNull(ctx.getSessionRoleOverride());

            // switcher ceiling: a role granted to the SERVICE, not the target
            Config.switch_user_role_ceiling = "switcher";
            new SuUserCommand(new UserIdentity("bobby", "%"),
                    Collections.singletonList("r_svc_only"), null).run(ctx, null);
            Assert.assertEquals(Collections.singleton("r_svc_only"), ctx.getSessionRoleOverride());
        } finally {
            Config.switch_user_users = savedUsers;
            Config.switch_user_role_ceiling = savedCeiling;
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testParserRoundTrip() {
        Object plan = new NereidsParser().parseSingle(
                "SU 'alice'@'%' WITH ROLES ('space_a', 'r2') WORKLOAD GROUP 'wg_space_5'");
        Assert.assertTrue(plan instanceof SuUserCommand);
        SuUserCommand cmd = (SuUserCommand) plan;
        Assert.assertEquals(2, cmd.getRoles().size());
        Assert.assertEquals("wg_space_5", cmd.getWorkloadGroup());

        Object plain = new NereidsParser().parseSingle("SU 'alice' WITH ROLES ('space_a')");
        Assert.assertTrue(plain instanceof SuUserCommand);
        Assert.assertNull(((SuUserCommand) plain).getWorkloadGroup());

        // `su` stays usable as an identifier (nonReserved)
        Object select = new NereidsParser().parseSingle("SELECT su FROM t1");
        Assert.assertNotNull(select);
    }
}
