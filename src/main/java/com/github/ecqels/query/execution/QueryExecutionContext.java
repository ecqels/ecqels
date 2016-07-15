/* 
 *  Copyright (C) 2016 Michael Jacoby.
 * 
 *  This library is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public 
 *  License as published by the Free Software Foundation, either 
 *  version 3 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this library.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.ecqels.query.execution;

import com.github.ecqels.lang.op.OpRefreshable;
import com.github.ecqels.lang.op.OpStream;
import com.github.ecqels.query.iterator.QueryIteratorCache;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.OpVisitorByTypeBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.Symbol;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class QueryExecutionContext {

    public static Symbol SYMBOL = Symbol.create(QueryExecutionContext.class.getName());

    private final QueryIteratorCache<Op> opCache;
    private final Map<Op, Op> hierachie = new HashMap<>();
    private final Set<Op> cachables = new HashSet<>();
    private final Set<Op> refreshedOps = new HashSet<>();
    private final Set<Op> opsAffectedFromRefresh = new HashSet<>();
    private final Set<Op> forceRefresh = new HashSet<>();

    public QueryExecutionContext(Op op, ExecutionContext context) {
        opCache = new QueryIteratorCache<>(context);
        buildOpHierarchie(op);
    }

    public boolean isForceRefresh(Op op) {
        return forceRefresh.contains(op);
    }

    public void setForceRefresh(Op op) {
        forceRefresh.add(op);
    }

    public void removeForceRefresh(Op op) {
        if (forceRefresh.contains(op)) {
            forceRefresh.remove(op);
        }
    }

    public QueryIteratorCache<Op> getCache() {
        return opCache;
    }

    public boolean isCacheable(Op op) {
        return cachables.contains(op);
    }

    public List<Op> getRefreshedOps() {
        return refreshedOps.stream().collect(Collectors.toList());
    }

    public boolean isRefreshed(Op op) {
        return refreshedOps.contains(op);
    }

    public boolean isAffectedFromRefresh(Op op) {
        return opsAffectedFromRefresh.contains(op);
    }

    public Op getParent(Op op) {
        if (hierachie.containsKey(op)) {
            return hierachie.get(op);
        }
        return null;
    }

    public List<Op> getChildren(Op op) {
        final List<Op> result = new ArrayList<Op>();
        op.visit(new OpVisitorByTypeBase() {
            Op parent = null;

            @Override
            protected void visitN(OpN op) {
                result.addAll(op.getElements());
            }

            @Override
            protected void visit2(Op2 op) {
                result.add(op.getLeft());
                result.add(op.getRight());
            }

            @Override
            protected void visit1(Op1 op) {
                result.add(op.getSubOp());
            }
        });
        return result;
    }

    private boolean checkContainsInstacesOf(Op op, Class... classes) {
        class CheckContainsRefreshableOpVisitor extends OpVisitorByType {

            public boolean containsRefreshable = false;
            private final Class[] classes;

            public CheckContainsRefreshableOpVisitor(Class[] classes) {
                this.classes = classes;
            }

            @Override
            protected void visitN(OpN op) {
                checkIfRefreshable(op);
            }

            @Override
            protected void visit2(Op2 op) {
                checkIfRefreshable(op);
            }

            @Override
            protected void visit1(Op1 op) {
                checkIfRefreshable(op);
            }

            @Override
            protected void visit0(Op0 op) {
                checkIfRefreshable(op);
            }

            @Override
            protected void visitExt(OpExt op) {
                checkIfRefreshable(op);
            }

            private void checkIfRefreshable(Op op) {
                for (Class clazz : classes) {
                    if (clazz.isInstance(op)) {
                        containsRefreshable = true;
                    }
                }
            }

            @Override
            protected void visitFilter(OpFilter op) {
                checkIfRefreshable(op);
            }

            @Override
            protected void visitLeftJoin(OpLeftJoin op) {
                checkIfRefreshable(op);
            }
        }
        CheckContainsRefreshableOpVisitor visitor = new CheckContainsRefreshableOpVisitor(classes);
        OpWalker.walk(op, visitor);
        return visitor.containsRefreshable;
    }

    private void buildOpHierarchie(Op op) {
        final List<Op> refreshables = new ArrayList<>();

        op.visit(new OpVisitorByTypeBase() {
            Op parent = null;

            @Override
            protected void visitN(OpN op) {
                hierachie.put(op, parent);
                BitSet childsAreRefreshable = new BitSet(op.getElements().size());
                for (int i = 0; i < op.getElements().size(); i++) {
                    Op child = op.getElements().get(i);
                    parent = op;
                    child.visit(this);
                    childsAreRefreshable.set(i, refreshables.contains(op.getElements().get(i)));
                }
                if (childsAreRefreshable.cardinality() >= 1) {
                    // if there are refreshables at all, check cachability
                    for (int i = 0; i < op.getElements().size(); i++) {
                        // check if there are others children that are refreshable
                        // if yes, than i might be cached
                        BitSet mySet = new BitSet(op.getElements().size());
                        mySet.set(i);
                        mySet.xor(childsAreRefreshable);
                        if (mySet.cardinality() > 0) {
                            cachables.add(op.getElements().get(i));
                        }
                    }
                }
            }

            @Override
            protected void visit2(Op2 op) {
                hierachie.put(op, parent);
                parent = op;
                op.getLeft().visit(this);
                parent = op;
                op.getRight().visit(this);
                if (refreshables.contains(op.getLeft())) {
                    cachables.add(op.getRight());
                }
                if (refreshables.contains(op.getRight())) {
                    cachables.add(op.getLeft());
                }
            }

            @Override
            protected void visit1(Op1 op) {
                hierachie.put(op, parent);
                if (op instanceof OpRefreshable) {
                    refreshables.add(op);
                }
                parent = op;
                op.getSubOp().visit(this);
                if (cachables.contains(op.getSubOp())) {
                    cachables.remove(op.getSubOp());
                    cachables.add(op);
                }
            }

            @Override
            protected void visit0(Op0 op) {
                hierachie.put(op, parent);
                cachables.add(op);
            }

            @Override
            protected void visitExt(OpExt op) {
                hierachie.put(op, parent);
                if (op instanceof OpStream) {
                    parent = op;
                    refreshables.add(op);
                    ((OpStream) op).getSubOp().visit(this);
                }
            }
        });
    }

    public void clearRefreshedOps() {
        refreshedOps.clear();
        opsAffectedFromRefresh.clear();
    }

    public void setRefreshedOps(List<Op> refreshedOps) {
        this.refreshedOps.clear();
        this.refreshedOps.addAll(refreshedOps);
        opsAffectedFromRefresh.clear();
        if (refreshedOps != null) {
            for (Op refreshedOp : refreshedOps) {
                opsAffectedFromRefresh.add(refreshedOp);
                Op temp = refreshedOp;
                while (hierachie.containsKey(temp)) {
                    temp = hierachie.get(temp);
                    if (!opsAffectedFromRefresh.contains(temp)) {
                        opsAffectedFromRefresh.add(temp);
                    }
                }
            }
        }
    }
}
