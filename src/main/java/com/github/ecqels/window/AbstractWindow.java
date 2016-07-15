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
package com.github.ecqels.window;

import com.github.ecqels.Engine;
import com.github.ecqels.event.DataChangedEvent;
import com.github.ecqels.event.DataChangedListener;
import com.github.ecqels.op.CachedOpExecutor;
import javax.swing.event.EventListenerList;
import org.apache.jena.graph.Node;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public abstract class AbstractWindow implements Window {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWindow.class);

    protected final DatasetGraph datasetGraph;
    protected final Node streamNode;
    protected final BasicPattern pattern;
    protected final Engine engine;
    protected EventListenerList listeners = new EventListenerList();
    protected boolean stop = false;
    protected OpGraph op;

    public AbstractWindow(Engine engine, Node streamNode, BasicPattern pattern) {
        this.engine = engine;
        this.streamNode = streamNode;
        this.pattern = pattern;
        this.datasetGraph = DatasetFactory.create().asDatasetGraph();
    }

    public QueryIterator evaluate() {
        return evaluate(QueryIterRoot.create(engine.getARQExecutionContext()), engine.getARQExecutionContext());
    }

    @Override
    public QueryIterator evaluate(QueryIterator input, ExecutionContext execCxt) {
        if (op == null) {
            op = new OpGraph(streamNode, new OpBGP(pattern));
        }
        ExecutionContext context = new ExecutionContext(execCxt.getContext(), null, datasetGraph, execCxt.getExecutor());
        //input = QueryIterRoot.create(context);
        //context = new ExecutionContext(datasetGraph.getContext(), datasetGraph.getDefaultGraph(), datasetGraph, OpExecutorTDB.OpExecFactoryTDB);
        //QueryIterator result = null;
        datasetGraph.getLock().enterCriticalSection(true);
        //QueryIterator result = QC.execute(op, input, context);
        QueryIterator result = CachedOpExecutor.OpExecFactoryTDB.create(context).executeOp(op, input);
        datasetGraph.getLock().leaveCriticalSection();
        return result;
    }

    @Override
    public Node getStreamNode() {
        return streamNode;
    }

    @Override
    public BasicPattern getPattern() {
        return pattern;
    }

    public void add(final Quad quad) {
        if (stop) {
            return;
        }
        datasetGraph.getLock().enterCriticalSection(false);
        datasetGraph.add(quad);
        datasetGraph.getLock().leaveCriticalSection();
    }

    @Override
    public DatasetGraph getDatasetGraph() {
        return datasetGraph;
    }

    @Override
    public void addDataChangedListener(DataChangedListener listener) {
        listeners.add(DataChangedListener.class, listener);
    }

    @Override
    public void removeDataChangedListener(DataChangedListener listener) {
        listeners.remove(DataChangedListener.class, listener);
    }

    protected void fireDataChanged(QueryIterator result) {
        if (stop) {
            return;
        }
        Object[] temp = listeners.getListenerList();
        for (int i = 0; i < temp.length; i = i + 2) {
            if (temp[i] == DataChangedListener.class) {
                ((DataChangedListener) temp[i + 1]).dataChanged(new DataChangedEvent(this, this, result));
            }
        }
    }

    @Override
    public void stop() {
        stop = true;
//        if (!statement.isStopped() && !statement.isDestroyed()) {
//            statement.destroy();
//        }
        datasetGraph.close();
    }

    public boolean equals(Object obj) {
        return !(obj == null || !(obj instanceof AbstractWindow));
    }

    public abstract Window clone();

}
