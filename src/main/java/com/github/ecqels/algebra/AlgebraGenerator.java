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
package com.github.ecqels.algebra;

import com.github.ecqels.lang.element.ElementRefreshableNamedGraph;
import com.github.ecqels.lang.element.ElementRefreshableService;
import com.github.ecqels.lang.element.ElementRefreshableStream;
import com.github.ecqels.lang.element.ElementStream;
import com.github.ecqels.lang.op.OpRefreshableGraph;
import com.github.ecqels.lang.op.OpRefreshableService;
import com.github.ecqels.lang.op.OpRefreshableStream;
import com.github.ecqels.lang.op.OpStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpAssign;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpList;
import org.apache.jena.sparql.algebra.op.OpMinus;
import org.apache.jena.sparql.algebra.op.OpNull;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpReduced;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.algebra.optimize.TransformSimplify;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.PathBlock;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_LogicalNot;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprLib;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.path.PathLib;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementAssign;
import org.apache.jena.sparql.syntax.ElementBind;
import org.apache.jena.sparql.syntax.ElementData;
import org.apache.jena.sparql.syntax.ElementExists;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementMinus;
import org.apache.jena.sparql.syntax.ElementNamedGraph;
import org.apache.jena.sparql.syntax.ElementNotExists;
import org.apache.jena.sparql.syntax.ElementOptional;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementService;
import org.apache.jena.sparql.syntax.ElementSubQuery;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.syntax.ElementUnion;
import org.apache.jena.sparql.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class AlgebraGenerator {

    // Fixed filter position means leave exactly where it is syntactically (illegal SPARQL)
    // Helpful only to write exactly what you mean and test the full query compiler.
    private boolean fixedFilterPosition = false;
    private Context context;
    private final int subQueryDepth;
    private static final Logger LOGGER = LoggerFactory.getLogger(AlgebraGenerator.class);

    static final private boolean applySimplification = true;
    static final private boolean simplifyTooEarlyInAlgebraGeneration = false;

    public AlgebraGenerator(Context context) {
        this(context != null ? context : ARQ.getContext().copy(), 0);
    }

    public AlgebraGenerator() {
        this(null);
    }

    private AlgebraGenerator(Context context, int depth) {
        this.context = context;
        this.subQueryDepth = depth;
    }

    public Op compile(Query query) {
        Op op = compile(query.getQueryPattern());

        op = compileModifiers(query, op);
        return op;
    }

    protected static Transform simplify = new TransformSimplify();

    // Compile any structural element
    public Op compile(Element elt) {
        Op op = compileElement(elt);
        Op op2 = op;
        if (!simplifyTooEarlyInAlgebraGeneration && applySimplification && simplify != null) {
            op2 = simplify(op);
        }
        return op2;
    }

    private static Op simplify(Op op) {
        return Transformer.transform(simplify, op);
    }

    protected Op compileElement(Element elt) {
        if (elt instanceof ElementRefreshableNamedGraph) {
            return compileElementRefreshableNamedGraph((ElementRefreshableNamedGraph) elt);
        }
        if (elt instanceof ElementRefreshableService) {
            return compileElementRefreshableService((ElementRefreshableService) elt);
        }
        if (elt instanceof ElementRefreshableStream) {
            return compileElementRefreshableStream((ElementRefreshableStream) elt);
        }
        if (elt instanceof ElementStream) {
            return compileElementStream((ElementStream) elt);
        }
        if (elt instanceof ElementGroup) {
            return compileElementGroup((ElementGroup) elt);
        }

        if (elt instanceof ElementUnion) {
            return compileElementUnion((ElementUnion) elt);
        }

        if (elt instanceof ElementNamedGraph) {
            return compileElementGraph((ElementNamedGraph) elt);
        }

        if (elt instanceof ElementService) {
            return compileElementService((ElementService) elt);
        }

        if (elt instanceof ElementTriplesBlock) {
            return compileBasicPattern(((ElementTriplesBlock) elt).getPattern());
        }

        if (elt instanceof ElementPathBlock) {
            return compilePathBlock(((ElementPathBlock) elt).getPattern());
        }

        if (elt instanceof ElementSubQuery) {
            return compileElementSubquery((ElementSubQuery) elt);
        }

        if (elt instanceof ElementData) {
            return compileElementData((ElementData) elt);
        }

        if (elt == null) {
            return OpNull.create();
        }

        broken("compile(Element)/Not a structural element: " + elt.getClass().getName());
        return null;
    }

    protected Op compileElementGroup(ElementGroup groupElt) {
        Pair<List<ElementFilter>, List<Element>> pair = prepareGroup(groupElt);
        List<ElementFilter> filters = pair.getLeft();
        List<Element> groupElts = pair.getRight();

        Op current = OpTable.unit();
        Deque<Op> acc = new ArrayDeque<Op>();

        for (Iterator<Element> iter = groupElts.listIterator(); iter.hasNext();) {
            Element elt = iter.next();
            if (elt != null) {
                current = compileOneInGroup(elt, current, acc);
            }
        }

        current = joinOpAcc(current, acc);

        if (filters != null) {
            for (ElementFilter filter : filters) {
                current = createFilter(filter, current);
            }
        }
        return current;
    }

    private Pair<List<ElementFilter>, List<Element>> prepareGroup(ElementGroup groupElt) {
        List<Element> groupElts = new ArrayList<Element>();

        PathBlock currentBGP = null;
        PathBlock currentPathBlock = null;
        List<ElementFilter> filters = null;

        for (Element elt : groupElt.getElements()) {
            if (!fixedFilterPosition && elt instanceof ElementFilter) {
                ElementFilter f = (ElementFilter) elt;
                if (filters == null) {
                    filters = new ArrayList<ElementFilter>();
                }
                filters.add(f);
                continue;
            }
            if (elt instanceof ElementTriplesBlock) {
                ElementTriplesBlock etb = (ElementTriplesBlock) elt;

                if (currentPathBlock == null) {
                    ElementPathBlock etb2 = new ElementPathBlock();
                    currentPathBlock = etb2.getPattern();
                    groupElts.add(etb2);
                }

                for (Triple t : etb.getPattern()) {
                    currentPathBlock.add(new TriplePath(t));
                }
                continue;
            }

            if (elt instanceof ElementPathBlock) {
                ElementPathBlock epb = (ElementPathBlock) elt;
                if (currentPathBlock == null) {
                    ElementPathBlock etb2 = new ElementPathBlock();
                    currentPathBlock = etb2.getPattern();
                    groupElts.add(etb2);
                }

                currentPathBlock.addAll(epb.getPattern());
                continue;
            }

            currentPathBlock = null;

            groupElts.add(elt);
        }
        return Pair.create(filters, groupElts);
    }

    private void accumulate(Deque<Op> acc, Op op) {
        acc.addLast(op);
    }

    private Op popAccumulated(Deque<Op> acc) {
        if (acc.size() == 0) {
            return OpTable.unit();
        }

        Op joined = null;
        for (Op op : acc) {
            joined = OpJoin.create(joined, op);
        }
        acc.clear();
        return joined;
    }

    private Op joinOpAcc(Op current, Deque<Op> acc) {
        if (acc.size() == 0) {
            return current;
        }
        Op joined = current;
        for (Op op : acc) {
            joined = OpJoin.create(joined, op);
        }
        acc.clear();
        return joined;
    }

    private Op compileOneInGroup(Element elt, Op current, Deque<Op> acc) {
        if (elt instanceof ElementTriplesBlock) {
            ElementTriplesBlock etb = (ElementTriplesBlock) elt;
            Op op = compileBasicPattern(etb.getPattern());
            accumulate(acc, op);
            return current;
        }

        if (elt instanceof ElementPathBlock) {
            ElementPathBlock epb = (ElementPathBlock) elt;
            Op op = compilePathBlock(epb.getPattern());
            accumulate(acc, op);
            return current;
        }

        if (elt instanceof ElementAssign) {
            Op op = popAccumulated(acc);
            ElementAssign assign = (ElementAssign) elt;
            Op opAssign = OpAssign.assign(op, assign.getVar(), assign.getExpr());
            accumulate(acc, opAssign);
            return current;
        }

        if (elt instanceof ElementBind) {
            Op op = popAccumulated(acc);
            ElementBind bind = (ElementBind) elt;
            Op opExtend = OpExtend.extend(op, bind.getVar(), bind.getExpr());
            accumulate(acc, opExtend);
            return current;
        }

        current = joinOpAcc(current, acc);

        if (elt instanceof ElementOptional) {
            ElementOptional eltOpt = (ElementOptional) elt;
            return compileElementOptional(eltOpt, current);
        }
        if (elt instanceof ElementMinus) {
            ElementMinus elt2 = (ElementMinus) elt;
            Op op = compileElementMinus(current, elt2);
            return op;
        }

        if (elt instanceof ElementGroup
                || elt instanceof ElementNamedGraph
                || elt instanceof ElementService
                || elt instanceof ElementUnion
                || elt instanceof ElementSubQuery
                || elt instanceof ElementData) {
            Op op = compileElement(elt);
            return join(current, op);
        }

        if (elt instanceof ElementExists) {
            ElementExists elt2 = (ElementExists) elt;
            Op op = compileElementExists(current, elt2);
            return op;
        }

        if (elt instanceof ElementNotExists) {
            ElementNotExists elt2 = (ElementNotExists) elt;
            Op op = compileElementNotExists(current, elt2);
            return op;
        }

        if (elt instanceof ElementFilter) {
            return createFilter((ElementFilter) elt, current);
        }

        broken("compile/Element not recognized: " + elt.getClass().getName());
        return null;
    }

    private Op compileElementUnion(ElementUnion el) {
        Op current = null;

        for (Element subElt : el.getElements()) {
            Op op = compileElement(subElt);
            current = union(current, op);
        }
        return current;
    }

    private Op compileElementNotExists(Op current, ElementNotExists elt2) {
        Op op = compile(elt2.getElement());
        Expr expr = new E_Exists(elt2, op);
        expr = new E_LogicalNot(expr);
        return OpFilter.filter(expr, current);
    }

    private Op compileElementExists(Op current, ElementExists elt2) {
        Op op = compile(elt2.getElement());
        Expr expr = new E_Exists(elt2, op);
        return OpFilter.filter(expr, current);
    }

    private Op createFilter(ElementFilter filter, Op current) {
        return OpFilter.filter(filter.getExpr(), current);
    }

    private Op compileElementMinus(Op current, ElementMinus elt2) {
        Op op = compile(elt2.getMinusElement());
        Op opMinus = OpMinus.create(current, op);
        return opMinus;
    }

    private Op compileElementData(ElementData elt) {
        return OpTable.create(elt.getTable());
    }

    private Op compileElementUnion(Op current, ElementUnion elt2) {
        Op op = compile(elt2.getElements().get(0));
        Op opUnion = OpUnion.create(current, op);
        return opUnion;
    }

    protected Op compileElementOptional(ElementOptional eltOpt, Op current) {
        Element subElt = eltOpt.getOptionalElement();
        Op op = compileElement(subElt);

        ExprList exprs = null;
        if (op instanceof OpFilter) {
            OpFilter f = (OpFilter) op;
            Op sub = f.getSubOp();
            if (sub instanceof OpFilter) {
                broken("compile/Optional/nested filters - unfinished");
            }
            exprs = f.getExprs();
            op = sub;
        }
        current = OpLeftJoin.create(current, op, exprs);
        return current;
    }

    protected Op compileBasicPattern(BasicPattern pattern) {
        return new OpBGP(pattern);
    }

    protected Op compilePathBlock(PathBlock pathBlock) {
        if (pathBlock.size() == 0) {
            return OpTable.unit();
        }
        return PathLib.pathToTriples(pathBlock);
    }

    protected Op compileElementGraph(ElementNamedGraph eltGraph) {
        Node graphNode = eltGraph.getGraphNameNode();
        Op sub = compileElement(eltGraph.getElement());
        return new OpGraph(graphNode, sub);
    }

    protected Op compileElementService(ElementService eltService) {
        Node serviceNode = eltService.getServiceNode();
        Op sub = compileElement(eltService.getElement());
        return new OpService(serviceNode, sub, eltService, eltService.getSilent());
    }

    protected Op compileElementSubquery(ElementSubQuery eltSubQuery) {
        return compile(eltSubQuery.getQuery());
    }

    protected Op compileElementRefreshableNamedGraph(ElementRefreshableNamedGraph refreshGraphElt) {
        return new OpRefreshableGraph((OpGraph) compileElementGraph(refreshGraphElt), refreshGraphElt.getDuration());
    }

    protected Op compileElementRefreshableService(ElementRefreshableService refreshServiceElt) {
        return new OpRefreshableService((OpService) compileElementService(refreshServiceElt), refreshServiceElt.getDuration());
    }

    protected Op compileElementRefreshableStream(ElementRefreshableStream refreshStreamElt) {
        return new OpRefreshableStream((OpStream) compileElementStream(refreshStreamElt), refreshStreamElt.getDuration());
    }

    protected Op compileElementStream(ElementStream streamElt) {
        Node graphNode = streamElt.getGraphNameNode();
        final BasicPattern pattern = new BasicPattern();
        final ElementGroup subElements = new ElementGroup();
        if (streamElt.getElement() instanceof ElementGroup) {
            for (Element element : ((ElementGroup) streamElt.getElement()).getElements()) {
                if (element instanceof ElementTriplesBlock) {
                    pattern.addAll(((ElementTriplesBlock) element).getPattern());
                } else if (element instanceof ElementPathBlock) {
                    for (TriplePath path : ((ElementPathBlock) element).getPattern()) {
                        pattern.add(path.asTriple());
                    }
                } else {
                    subElements.addElement(element);
                }
            }
        } else if (streamElt.getElement() instanceof ElementTriplesBlock) {
            pattern.addAll(((ElementTriplesBlock) streamElt.getElement()).getPattern());
        } else if (streamElt.getElement() instanceof ElementPathBlock) {
            for (TriplePath path : ((ElementPathBlock) streamElt.getElement()).getPattern()) {
                pattern.add(path.asTriple());
            }
        } else {
            subElements.addElement(streamElt.getElement());
        }
        Op sub = compileElement(subElements);
        return new OpStream(graphNode, sub, pattern, streamElt.getWindowInfo());
    }

    private Op compileModifiers(Query query, Op pattern) {
        VarExprList projectVars = query.getProject();

        VarExprList exprs = new VarExprList();
        List<Var> vars = new ArrayList<Var>();

        Op op = pattern;

        if (query.hasGroupBy()) {
            op = new OpGroup(op, query.getGroupBy(), query.getAggregators());
        }

        if (!projectVars.isEmpty() && !query.isQueryResultStar()) {

            if (projectVars.size() == 0 && query.isSelectType()) {
                Log.warn(this, "No project variables");
            }
            for (Var v : query.getProject().getVars()) {
                Expr e = query.getProject().getExpr(v);
                if (e != null) {
                    Expr e2 = ExprLib.replaceAggregateByVariable(e);
                    exprs.add(v, e2);
                }
                vars.add(v);
            }
        }

        if (!exprs.isEmpty()) {
            op = OpExtend.extend(op, exprs);
        }

        if (query.hasHaving()) {
            for (Expr expr : query.getHavingExprs()) {
                Expr expr2 = ExprLib.replaceAggregateByVariable(expr);
                op = OpFilter.filter(expr2, op);
            }
        }
        if (query.hasValues()) {
            Table table = TableFactory.create(query.getValuesVariables());
            for (Binding binding : query.getValuesData()) {
                table.addBinding(binding);
            }
            OpTable opTable = OpTable.create(table);
            op = OpJoin.create(op, opTable);
        }

        if (context.isTrue(ARQ.generateToList)) {
            op = new OpList(op);
        }

        if (query.getOrderBy() != null) {
            List<SortCondition> scList = new ArrayList<SortCondition>();
            for (SortCondition sc : query.getOrderBy()) {
                Expr e = sc.getExpression();
                e = ExprLib.replaceAggregateByVariable(e);
                scList.add(new SortCondition(e, sc.getDirection()));

            }
            op = new OpOrder(op, scList);
        }
        if (vars.size() > 0) {
            op = new OpProject(op, vars);
        }
        if (query.isDistinct()) {
            op = OpDistinct.create(op);
        }
        if (query.isReduced()) {
            op = OpReduced.create(op);
        }
        if (query.hasLimit() || query.hasOffset()) {
            op = new OpSlice(op, query.getOffset(), query.getLimit());
        }

        return op;
    }

    private static Op join(Op current, Op newOp) {
        if (simplifyTooEarlyInAlgebraGeneration && applySimplification) {
            return OpJoin.createReduce(current, newOp);
        }

        return OpJoin.create(current, newOp);
    }

    protected Op sequence(Op current, Op newOp) {
        return OpSequence.create(current, newOp);
    }

    protected Op union(Op current, Op newOp) {
        return OpUnion.create(current, newOp);
    }

    private void broken(String msg) {
        throw new ARQInternalErrorException(msg);
    }
}
