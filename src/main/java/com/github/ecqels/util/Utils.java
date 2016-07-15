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
package com.github.ecqels.util;

import com.github.ecqels.lang.op.OpRefreshableGraph;
import com.github.ecqels.lang.op.OpRefreshableService;
import com.github.ecqels.lang.op.OpStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.OpWalker.WalkerVisitor;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

public class Utils {

    public static Node toNode(String st) {
        return NodeFactory.createURI(st);
    }

    public static <R, T extends R> List<R> findInstacesOf(Op op, Class<T> type) {
        class CheckContainsInstanceOfOpVisitor extends OpVisitorByType {

            private final Class<T> type;
            public List<R> result = new ArrayList<>();

            public CheckContainsInstanceOfOpVisitor(Class<T> type) {
                this.type = type;
            }

            @Override
            protected void visitN(OpN op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit2(Op2 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit1(Op1 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit0(Op0 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visitExt(OpExt op) {
                checkIfIsInstanceOf(op);
            }

            private void checkIfIsInstanceOf(Op op) {
                if (type.isAssignableFrom(op.getClass())) {
                    result.add((T) op);
                }
            }

            @Override
            protected void visitFilter(OpFilter op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visitLeftJoin(OpLeftJoin op) {
                checkIfIsInstanceOf(op);
            }
        }
        CheckContainsInstanceOfOpVisitor visitor = new CheckContainsInstanceOfOpVisitor(type);
        op.visit(new WalkerVisitor(visitor) {
            @Override
            protected void visitExt(OpExt op) {
                if (op instanceof OpStream) {
                    ((OpStream) op).getSubOp().visit(this);
                }
                if (op instanceof OpRefreshableGraph) {
                    ((OpRefreshableGraph) op).getOp().visit(this);
                }
                if (op instanceof OpRefreshableService) {
                    ((OpRefreshableService) op).getOp().visit(this);
                }
                super.visitExt(op);
            }
        });
        return visitor.result;
    }

    public static boolean isSubOp(final Op subOp, final Op parent) {
        class OpVisitorFindOp extends OpVisitorByType {

            public boolean containsOp = false;

            @Override
            protected void visitN(OpN op) {
                if (op.equals(subOp)) {
                    containsOp = true;
                }
            }

            @Override
            protected void visit2(Op2 op) {
                if (op.equals(subOp)) {
                    containsOp = true;
                }
            }

            @Override
            protected void visit1(Op1 op) {
                if (op.equals(subOp)) {
                    containsOp = true;
                }
            }

            @Override
            protected void visit0(Op0 op) {
                if (op.equals(subOp)) {
                    containsOp = true;
                }
            }

            @Override
            protected void visitExt(OpExt op) {
                if (op.equals(subOp)) {
                    containsOp = true;
                }
            }

            @Override
            protected void visitFilter(OpFilter op) {
                if (op.equals(subOp)) {
                    containsOp = true;
                }
            }

            @Override
            protected void visitLeftJoin(OpLeftJoin op) {
                if (op.equals(subOp)) {
                    containsOp = true;
                }
            }
        }
        OpVisitorFindOp visitor = new OpVisitorFindOp();
        OpWalker.walk(parent, visitor);
        return visitor.containsOp;
    }

    public static boolean checkContainsInstacesOf(Op op, Class... classes) {
        class CheckContainsInstanceOfOpVisitor extends OpVisitorByType {

            public boolean containsInstanceOf = false;
            private final Class[] classes;

            public CheckContainsInstanceOfOpVisitor(Class[] classes) {
                this.classes = classes;
            }

            @Override
            protected void visitN(OpN op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit2(Op2 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit1(Op1 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit0(Op0 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visitExt(OpExt op) {
                checkIfIsInstanceOf(op);
            }

            private void checkIfIsInstanceOf(Op op) {
                for (Class clazz : classes) {
                    if (clazz.isInstance(op)) {
                        containsInstanceOf = true;
                    }
                }
            }

            @Override
            protected void visitFilter(OpFilter op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visitLeftJoin(OpLeftJoin op) {
                checkIfIsInstanceOf(op);
            }
        }
        CheckContainsInstanceOfOpVisitor visitor = new CheckContainsInstanceOfOpVisitor(classes);
        OpWalker.walk(op, visitor);
        return visitor.containsInstanceOf;
    }

    public static ArrayList<Var> quad2Vars(Quad quad) {
        ArrayList<Var> vars = new ArrayList<Var>();
        if (quad.getGraph().isVariable()) {
            vars.add((Var) quad.getGraph());
        }
        if (quad.getSubject().isVariable()) {
            vars.add((Var) quad.getSubject());
        }
        if (quad.getPredicate().isVariable()) {
            vars.add((Var) quad.getPredicate());
        }
        if (quad.getObject().isVariable()) {
            vars.add((Var) quad.getObject());
        }
        return vars;
    }
}
