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

import com.github.ecqels.lang.window.Duration;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.sse.writers.WriterOp;
import org.apache.jena.sparql.syntax.ElementService;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

/**
 *
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email michael.jacoby@iosb.fraunhofer.de
 */
public class OpRefreshableService extends OpExt implements OpRefreshable {

    private final Duration duration;
    private final OpService op;

    public OpRefreshableService(Node node, Op subOp, ElementService elt, boolean silent, Duration duration) {
        this(new OpService(node, subOp, elt, silent), duration);
    }

    public OpRefreshableService(OpService op, Duration duration) {
        super("service");
        this.op = op;
        this.duration = duration;
    }

    public OpService getOp() {
        return op;
    }

    public Duration getDuration() {
        return duration;
    }

    @Override
    public Op effectiveOp() {
        return op;
    }

    @Override
    public QueryIterator eval(QueryIterator input, ExecutionContext execCxt) {
        return QC.execute(op, input, execCxt);
    }

    @Override
    public void outputArgs(IndentedWriter out, SerializationContext sCxt) {
        out.print("[REFRESH " + duration + "]");
        WriterOp.output(out, op, sCxt);
    }

    @Override
    public int hashCode() {
        return op.hashCode() ^ duration.hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpRefreshableService)) {
            return false;
        }
        OpRefreshableService otherOp = (OpRefreshableService) other;
        return otherOp.op.equalTo(op, labelMap) && otherOp.duration.equals(duration);
    }
}
