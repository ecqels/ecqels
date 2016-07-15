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
package com.github.ecqels.lang.op;

import com.github.ecqels.lang.window.WindowInfo;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.main.VarFinder;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.sse.writers.WriterOp;
import org.apache.jena.sparql.util.FmtUtils;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

public class OpStream extends OpExt {

    protected final WindowInfo windowInfo;
    protected final Node node;
    protected final Op subOp;
    protected final BasicPattern pattern;

    public OpStream(Node node, Op subOp, BasicPattern pattern, WindowInfo windowInfo) {
        super("stream");
        if (node.isVariable() && !VarFinder.fixed(subOp).contains(node)) {
            throw new IllegalArgumentException("node '" + node + "' is variable but is not declared in suboperation '" + subOp + "'");
        }
        this.windowInfo = windowInfo;
        this.node = node;
        this.subOp = subOp;
        this.pattern = pattern;
    }

    public WindowInfo getWindowInfo() {
        return windowInfo;
    }

    public Node getNode() {
        return node;
    }

    public BasicPattern getPattern() {
        return pattern;
    }

    public Op getSubOp() {
        return subOp;
    }

    @Override
    public Op effectiveOp() {
        // TODO integrate pattern
        return OpJoin.create(new OpGraph(node, new OpBGP(pattern)), subOp);
    }

    @Override
    public QueryIterator eval(QueryIterator input, ExecutionContext execCxt) {
//        if (execCxt.getContext().isDefined(QueryExecutionContext.SYMBOL)) {
//            QueryExecutionContext queryContext = (QueryExecutionContext) execCxt.getContext().get(QueryExecutionContext.SYMBOL);
//            QueryIterConcat result = new QueryIterConcat(execCxt);
//            // execute on every stream
//            for (Map.Entry<Node, Window> stream : queryContext.getCurrentStreamBindings().entrySet()) {
//                OpGraph op = new OpGraph(stream.getKey(), new OpBGP(pattern));
//                ExecutionContext context = new ExecutionContext(execCxt.getContext(), null, stream.getValue().getDatasetGraph(), execCxt.getExecutor());
//                //result.add(QC.execute(op, currentUris, context));
//                result.add(QC.execute(op, input, context));
//            }
//            return result;
//        }
//        return QC.execute(this, input, execCxt);
        return new QueryIterNullIterator(execCxt);
        //return QueryIterRoot.create(execCxt);
    }

    @Override
    public void outputArgs(IndentedWriter out, SerializationContext sCxt) {
        out.println(FmtUtils.stringForNode(node, sCxt) + "[" + windowInfo.toString() + "] {" + pattern.toString() + "}");
        out.ensureStartOfLine();
        WriterOp.output(out, subOp, sCxt);
    }

    @Override
    public int hashCode() {
        return node.hashCode() ^ /*pattern.hashCode() ^*/ windowInfo.hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpStream)) {
            return false;
        }
        OpStream otherOp = (OpStream) other;
        return otherOp.node.equals(node) && otherOp.subOp.equalTo(subOp, labelMap) && otherOp.windowInfo.equals(windowInfo);
    }
}
