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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUsingJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * The user property {@code hide_inaccessible_columns}: a {@code *} in the select list of a query expands only to
 * the columns the user may read, where it would otherwise fail on the first column the user may not read.
 *
 * <p>It only ever removes columns from a star. A statement that names a column the user may not read is checked
 * as before, and so is everything the star still expands to.
 *
 * <ul>
 * <li>Only a star that sits directly over tables and views is narrowed: through an alias, a filter and a join,
 *     not through a subquery, a CTE or a set operation, which have a select list of their own. The star inside
 *     them is narrowed where it is bound.</li>
 * <li>Only in a query whose rows go back to the client. INSERT ... SELECT, CREATE TABLE AS SELECT, CREATE VIEW,
 *     an export or a load would store the narrowed column list, so they keep the refusal.</li>
 * <li>Not inside the body of a view: a view is read with its own authority.</li>
 * <li>Not in a session switched by SU: the property follows the user's own login.</li>
 * <li>A table or view of which the user may read no column is left whole, so the statement fails as before.</li>
 * </ul>
 */
public final class HideInaccessibleColumns {

    private HideInaccessibleColumns() {
    }

    /**
     * The slots under {@code starOwner} which a star bound in it must not expand to. Empty unless the property is
     * set for the user of this session and the statement is one a star is narrowed in.
     */
    public static Set<ExprId> inaccessibleSlots(Plan starOwner, CascadesContext cascadesContext) {
        if (!appliesTo(cascadesContext)) {
            return ImmutableSet.of();
        }
        ConnectContext connectContext = cascadesContext.getConnectContext();
        StatementContext statementContext = cascadesContext.getStatementContext();
        Set<ExprId> inaccessible = Sets.newHashSet();
        for (Plan child : starOwner.children()) {
            collect(child, connectContext, statementContext, inaccessible);
        }
        if (!inaccessible.isEmpty()) {
            // The same text gives another column list to another user, and to this user once a grant changes.
            statementContext.getSqlCacheContext().ifPresent(cache -> cache.setHasUnsupportedTables(true));
        }
        return inaccessible;
    }

    private static boolean appliesTo(CascadesContext cascadesContext) {
        ConnectContext connectContext = cascadesContext.getConnectContext();
        if (connectContext == null || connectContext.isSkipAuth() || connectContext.getQualifiedUser() == null) {
            return false;
        }
        // A session switched by SU acts for the user through a narrowed role set, and its callers rely on the
        // refusal to learn that a statement reaches beyond that set. The property follows the user's own login only.
        if (connectContext.getAuthenticatedIdentity() != null) {
            return false;
        }
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (statementContext == null || statementContext.isAnalyzingViewBody()
                || !returnsRowsToClient(statementContext.getParsedStatement())) {
            return false;
        }
        return connectContext.getEnv().getAuth().isHideInaccessibleColumns(connectContext.getQualifiedUser());
    }

    /** A query, or EXPLAIN of a query, whose result set goes back to the client. */
    private static boolean returnsRowsToClient(StatementBase parsedStatement) {
        if (!(parsedStatement instanceof LogicalPlanAdapter)) {
            return false;
        }
        LogicalPlan plan = ((LogicalPlanAdapter) parsedStatement).getLogicalPlan();
        if (plan instanceof ExplainCommand) {
            plan = ((ExplainCommand) plan).getLogicalPlan();
        }
        return plan instanceof UnboundResultSink;
    }

    private static void collect(Plan plan, ConnectContext connectContext, StatementContext statementContext,
            Set<ExprId> inaccessible) {
        if (plan instanceof LogicalView) {
            // the view's own columns and the grants on the view; its body is not looked into
            hide(((LogicalView<?>) plan).getView(), plan.getOutput(), connectContext, statementContext, inaccessible);
        } else if (plan instanceof LogicalCatalogRelation) {
            hide(((LogicalCatalogRelation) plan).getTable(), plan.getOutput(), connectContext, statementContext,
                    inaccessible);
        } else if (plan instanceof LogicalSubQueryAlias || plan instanceof LogicalFilter
                || plan instanceof LogicalJoin || plan instanceof LogicalUsingJoin
                || plan instanceof LogicalCheckPolicy) {
            for (Plan child : plan.children()) {
                collect(child, connectContext, statementContext, inaccessible);
            }
        }
        // Every other node has a select list of its own or changes the rows. What a star over it expands to was
        // decided below it, and a column named there is checked as before.
    }

    private static void hide(TableIf table, List<Slot> output, ConnectContext connectContext,
            StatementContext statementContext, Set<ExprId> inaccessible) {
        List<Slot> columns = Lists.newArrayListWithCapacity(output.size());
        Set<String> names = Sets.newLinkedHashSetWithExpectedSize(output.size());
        for (Slot slot : output) {
            // a hidden column, e.g. __DORIS_DELETE_SIGN__, is neither in a star nor checked
            if (slot instanceof SlotReference && !((SlotReference) slot).isVisible()) {
                continue;
            }
            columns.add(slot);
            names.add(slot.getName());
        }
        if (columns.isEmpty() || mayRead(table, names, connectContext)) {
            return;
        }
        List<Slot> denied = Lists.newArrayList();
        for (Slot column : columns) {
            if (!mayRead(table, ImmutableSet.of(column.getName()), connectContext)) {
                denied.add(column);
            }
        }
        if (denied.size() == columns.size()) {
            // nothing of it may be read: the star stays whole and the statement is refused as before
            return;
        }
        for (Slot column : denied) {
            inaccessible.add(column.getExprId());
            statementContext.addHiddenInaccessibleColumn(table.getName() + "." + column.getName());
        }
    }

    private static boolean mayRead(TableIf table, Set<String> columns, ConnectContext connectContext) {
        try {
            UserAuthentication.checkPermission(table, connectContext, columns);
            return true;
        } catch (UserException | AnalysisException e) {
            return false;
        }
    }
}
