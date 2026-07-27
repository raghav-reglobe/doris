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

import org.apache.doris.alter.AlterUserOpType;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterUserCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.persist.AlterUserOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * MySQL-compatible dual password:
 * ALTER USER ... IDENTIFIED BY ... RETAIN CURRENT PASSWORD keeps the previous
 * password valid (secondary slot) until the next password change without
 * RETAIN, or an explicit ALTER USER ... DISCARD OLD PASSWORD.
 */
public class DualPasswordTest {

    private Auth auth;
    private Env env = Mockito.mock(Env.class);
    private EditLog editLog = Mockito.mock(EditLog.class);
    private MockedStatic<Env> mockedEnvStatic;

    @Before
    public void setUp() throws Exception {
        auth = new Auth();
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
    }

    private UserIdentity createUser(String name) throws DdlException {
        UserIdentity userIdentity = new UserIdentity(name, "%");
        userIdentity.setIsAnalyzed();
        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(new UserDesc(userIdentity)));
        auth.createUser(createUserCommand.getInfo());
        return userIdentity;
    }

    private boolean canLogin(String user, String plainPassword) {
        try {
            auth.checkPlainPassword(user, "192.168.1.1", plainPassword, null);
            return true;
        } catch (AuthenticationException e) {
            return false;
        }
    }

    @Test
    public void testRetainEvictAndDiscard() throws DdlException {
        UserIdentity user = createUser("rot");

        // initial password p1
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        Assert.assertTrue(canLogin("rot", "p1"));
        Assert.assertFalse(canLogin("rot", "p2"));

        // p2 RETAIN CURRENT PASSWORD -> p1 and p2 both authenticate
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("rot", "p2"));
        Assert.assertTrue(canLogin("rot", "p1"));
        Assert.assertFalse(canLogin("rot", "p0"));

        // p3 RETAIN -> the one-secondary rule evicts p1; p2 + p3 authenticate
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p3"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("rot", "p3"));
        Assert.assertTrue(canLogin("rot", "p2"));
        Assert.assertFalse(canLogin("rot", "p1"));

        // p4 WITHOUT retain -> the secondary REMAINS UNCHANGED (MySQL: "the
        // secondary password remains unchanged"); the replaced primary p3 is
        // simply gone -> p4 + p2 authenticate, p3 does not
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p4"), null,
                true, false, false /* no retain */, false);
        Assert.assertTrue(canLogin("rot", "p4"));
        Assert.assertFalse(canLogin("rot", "p3"));
        Assert.assertTrue(canLogin("rot", "p2"));

        // p5 RETAIN, then DISCARD OLD PASSWORD (via the replay path, which is
        // also what a follower executes) -> only p5 remains
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p5"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("rot", "p4"));
        auth.replayAlterUser(new AlterUserOperationLog(AlterUserOpType.DISCARD_OLD_PASSWORD,
                user, null, null, PasswordOptions.UNSET_OPTION, null));
        Assert.assertTrue(canLogin("rot", "p5"));
        Assert.assertFalse(canLogin("rot", "p4"));

        // DISCARD with no secondary present: silent no-op (MySQL: discards
        // the secondary password, "if one exists")
        auth.replayAlterUser(new AlterUserOperationLog(AlterUserOpType.DISCARD_OLD_PASSWORD,
                user, null, null, PasswordOptions.UNSET_OPTION, null));
        Assert.assertTrue(canLogin("rot", "p5"));
    }

    @Test
    public void testRetainRequiresNonEmptyCurrentPassword() throws DdlException {
        UserIdentity user = createUser("empty_cur");
        // MySQL: "If you specify RETAIN CURRENT PASSWORD for an account that
        // has an empty primary password, the statement fails."
        Assert.assertThrows(DdlException.class, () -> auth.setPasswordInternal(user,
                MysqlPassword.makeScrambledPassword("p1"), null, true, false, true /* retain */, false));
    }

    @Test
    public void testEmptyNewPasswordEmptiesSecondary() throws DdlException {
        UserIdentity user = createUser("empty_new");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("empty_new", "p1"));

        // MySQL: "If the new password ... is empty, the secondary password
        // becomes empty as well, even if RETAIN CURRENT PASSWORD is given."
        auth.setPasswordInternal(user, new byte[0], null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("empty_new", ""));
        Assert.assertFalse(canLogin("empty_new", "p1"));
        Assert.assertFalse(canLogin("empty_new", "p2"));
    }

    @Test
    public void testRetainReplay() throws DdlException {
        UserIdentity user = createUser("replayer");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));

        // a follower replaying OP_SET_PASSWORD with retainPasswd=true must
        // reach the same dual-slot state as the master
        auth.replaySetPassword(new PrivInfo(user, null,
                MysqlPassword.makeScrambledPassword("p2"), null, null, true /* retain */));
        Assert.assertTrue(canLogin("replayer", "p2"));
        Assert.assertTrue(canLogin("replayer", "p1"));

        // and a replay without the flag (all journals written before this
        // feature, plus any plain password change) behaves like a plain
        // change: the primary is replaced, the secondary remains UNCHANGED
        // (MySQL: "the secondary password remains unchanged")
        auth.replaySetPassword(new PrivInfo(user, null,
                MysqlPassword.makeScrambledPassword("p3"), null, null));
        Assert.assertTrue(canLogin("replayer", "p3"));
        Assert.assertFalse(canLogin("replayer", "p2"));
        Assert.assertTrue(canLogin("replayer", "p1"));
    }

    @Test
    public void testGsonCompat() {
        // round trip preserves the secondary slot
        Password password = new Password(MysqlPassword.makeScrambledPassword("p2"));
        password.setSecondaryPassword(MysqlPassword.makeScrambledPassword("p1"));
        Password reloaded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(password), Password.class);
        Assert.assertArrayEquals(password.getPassword(), reloaded.getPassword());
        Assert.assertArrayEquals(password.getSecondaryPassword(), reloaded.getSecondaryPassword());
        Assert.assertTrue(reloaded.hasSecondaryPassword());

        // an image/journal written BEFORE this feature deserializes with an
        // absent secondary slot -> unchanged single-password behavior
        Password legacy = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(new Password(MysqlPassword.makeScrambledPassword("p1"))), Password.class);
        Assert.assertFalse(legacy.hasSecondaryPassword());
        Assert.assertNull(legacy.getSecondaryPassword());
    }

    @Test
    public void testParser() {
        NereidsParser parser = new NereidsParser();

        AlterUserCommand retain = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD");
        Assert.assertTrue(retain.getAlterUserInfo().isRetainCurrentPassword());
        Assert.assertFalse(retain.getAlterUserInfo().isDiscardOldPassword());

        AlterUserCommand discard = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 DISCARD OLD PASSWORD");
        Assert.assertTrue(discard.getAlterUserInfo().isDiscardOldPassword());
        Assert.assertFalse(discard.getAlterUserInfo().isRetainCurrentPassword());

        AlterUserCommand plain = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 IDENTIFIED BY 'x'");
        Assert.assertFalse(plain.getAlterUserInfo().isRetainCurrentPassword());
        Assert.assertFalse(plain.getAlterUserInfo().isDiscardOldPassword());

        // DISCARD and OLD are nonReserved: still valid as identifiers
        parser.parseSingle("SELECT old, discard FROM discard.old");
    }

    @Test
    public void testValidateRejectsRetainWithoutPasswordChange() {
        // the clause parses on its own; the semantic rejection (RETAIN
        // requires a password change) lives in AlterUserInfo.validate()
        NereidsParser parser = new NereidsParser();
        AlterUserCommand cmd = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 RETAIN CURRENT PASSWORD");
        Assert.assertTrue(cmd.getAlterUserInfo().isRetainCurrentPassword());
        Assert.assertThrows(AnalysisException.class, () -> cmd.getAlterUserInfo().validate());
    }

    @Test
    public void testSetPasswordRetainOverload() throws DdlException {
        // SET PASSWORD ... RETAIN CURRENT PASSWORD routes through
        // Auth.setPassword(user, pw, retain=true) — the self-service path a
        // service account uses to rotate its own credential with an overlap.
        UserIdentity user = createUser("setpw");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        Assert.assertTrue(canLogin("setpw", "p1"));

        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p2"), true /* retain */);
        Assert.assertTrue(canLogin("setpw", "p2"));         // new primary
        Assert.assertTrue(canLogin("setpw", "p1"));         // retained secondary

        // a plain SET PASSWORD (retain=false) preserves the secondary (MySQL)
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p3"), false);
        Assert.assertTrue(canLogin("setpw", "p3"));
        Assert.assertTrue(canLogin("setpw", "p1"));         // still valid
        Assert.assertFalse(canLogin("setpw", "p2"));        // replaced primary gone
    }

    @Test
    public void testSetPasswordRetainParses() {
        // The clause parses on SET PASSWORD; no exception, RETAIN accepted.
        NereidsParser parser = new NereidsParser();
        parser.parseSingle("SET PASSWORD = PASSWORD('x') RETAIN CURRENT PASSWORD");
        parser.parseSingle("SET PASSWORD FOR u1 = PASSWORD('x') RETAIN CURRENT PASSWORD");
        // still parses WITHOUT the clause (back-compat)
        parser.parseSingle("SET PASSWORD = PASSWORD('x')");
    }
}
