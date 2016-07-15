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

import com.github.ecqels.Engine;
import com.github.ecqels.algebra.Algebra;
import com.github.ecqels.event.NewQueryResultAvailableEvent;
import com.github.ecqels.event.NewQueryResultAvailableListener;
import com.github.ecqels.event.RefreshRequestedEvent;
import com.github.ecqels.event.RefreshRequestedListener;
import com.github.ecqels.lang.op.OpRefreshable;
import com.github.ecqels.lang.op.OpRefreshableGraph;
import com.github.ecqels.lang.op.OpRefreshableService;
import com.github.ecqels.lang.op.OpStream;
import com.github.ecqels.op.CachedOpExecutor;
import com.github.ecqels.query.iterator.QueryIteratorCopy;
import com.github.ecqels.refresh.RefreshManager;
import com.github.ecqels.refresh.RefreshRequest;
import com.github.ecqels.refresh.RefreshRequest.RefreshRequestSource;
import com.github.ecqels.refresh.UpdateRefreshIntervalsTransform;
import com.github.ecqels.stream.StreamExecutor;
import com.github.ecqels.util.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import javax.swing.event.EventListenerList;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.jena.sparql.util.NodeIsomorphismMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class QueryExecutor implements RefreshRequestedListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    public static final Var ABORT_QUERY_VARIABLE = Var.alloc("ABORT_QUERY");
    protected boolean stop = false;
    protected Engine engine;
    protected ExecutionContext executionContext;
    protected QueryExecutionContext queryExecutionContext;
    protected Query query;
    protected Op op;
    protected EventListenerList listeners = new EventListenerList();
    protected final List<StreamExecutor> streams = new ArrayList<>();
    protected ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    protected final BlockingQueue<RefreshRequest> refreshRequestQueue = new LinkedBlockingQueue<>();
    protected List<OpRefreshable> refreshables;
    protected final RefreshManager refreshManager = new RefreshManager();
    protected BindingMap initialBinding = BindingFactory.create();
    private final Map<Node, List<StreamExecutor>> streamRegistrations = new HashMap<>();

    public QueryExecutor(Engine engine, Query query) {
        this(engine, query, new HashMap<>());
    }

    public QueryExecutor(Engine engine, Query query, Map<String, String> variableBindings) {
        this.engine = engine;
        this.query = query;

        //Op opOptimzed = Algebra.optimize(Algebra.compile(query));
        Op opOptimzed = Algebra.compile(query);
        this.op = Transformer.transform(new UpdateRefreshIntervalsTransform(opOptimzed), opOptimzed);
        this.refreshManager.addRefreshRequestedListener(this);
        initVariableBindings(variableBindings);
        init();
    }

    public void registerToStream(Node stream, StreamExecutor executor) {
        if (!streamRegistrations.containsKey(stream)) {
            streamRegistrations.put(stream, new ArrayList<>());
        }
        streamRegistrations.get(stream).add(executor);
    }

    public void unregisterFromStream(Node stream, StreamExecutor executor) {
        if (streamRegistrations.containsKey(stream)) {
            streamRegistrations.get(stream).remove(executor);
        }
    }

    public void send(Node graph, Node s, Node p, Node o) {
        RefreshRequest request = RefreshRequest.empty();
        if (streamRegistrations.containsKey(graph)) {
            for (StreamExecutor stream : streamRegistrations.get(graph)) {
                request.addSource(stream.send(graph, s, p, o));
            }
        }
        if (!request.getSources().isEmpty()) {
            // need to update
            execute(request);
        }
    }

    private void initVariableBindings(Map<String, String> variableBindings) {
        for (Map.Entry<String, String> binding : variableBindings.entrySet()) {
            //check type and parse content of bindinge for URI, literal, etc
            initialBinding.add(Var.alloc(binding.getKey()), NodeFactoryExtra.parseNode(binding.getValue()));
        }
    }

    protected final boolean isStop() {
        return stop;
    }

    public void start() {
        startRefreshables();
        startStreams();
        refreshManager.start();
    }

    public void stop() {
        stop = true;
        refreshManager.stop();
        streams.stream().forEach((stream) -> stream.stop());
        executor.shutdownNow();
    }

    private void init() {
        buildContexts();
        buildStreams();
        buildRefreshables();
        executor.execute(() -> {
            while (!isStop()) {
                try {
                    execute(refreshRequestQueue.take());
                } catch (Exception ex) {
                    //Logger.getLogger(QueryExecutor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        });
    }

    protected void buildContexts() {
        executionContext = new ExecutionContext(new Context(), engine.getARQExecutionContext().getActiveGraph(), engine.getARQExecutionContext().getDataset(), CachedOpExecutor.CQELSOpExecFactory);
        queryExecutionContext = new QueryExecutionContext(op, executionContext);
        executionContext.getContext().set(QueryExecutionContext.SYMBOL, queryExecutionContext);
    }

    protected void buildStreams() {
        List<OpStream> streamOpsTemp = Utils.findInstacesOf(op, OpStream.class);
        List<List<OpStream>> streamOps = new ArrayList<>();
        // group stream by same node and window AND subops
        for (OpStream stream : streamOpsTemp) {
            boolean isAdded = false;
            for (List<OpStream> streams : streamOps) {
                if (!streams.isEmpty()) {
                    if (streams.get(0).equalTo(stream, new NodeIsomorphismMap())) {
                        streams.add(stream);
                        isAdded = true;
                        break;
                    }
                }
            }
            if (!isAdded) {
                List<OpStream> newList = new ArrayList<>();
                newList.add(stream);
                streamOps.add(newList);
            }
        }
        for (List<OpStream> streamOp : streamOps) {
            StreamExecutor stream = new StreamExecutor(engine, executionContext, this, refreshManager, streamOp);
            stream.addRefreshRequestedListener(this);
            streams.add(stream);
        }
    }

    protected void buildRefreshables() {
        refreshables = Utils.findInstacesOf(op, OpRefreshableGraph.class);
        refreshables.addAll(Utils.<OpRefreshable, OpRefreshableService>findInstacesOf(op, OpRefreshableService.class));
    }

    protected void startStreams() {
        for (StreamExecutor stream : streams) {
            stream.start();
        }
    }

    protected void startRefreshables() {
        for (OpRefreshable refreshable : refreshables) {
            refreshManager.schedule(new Callable<RefreshRequest>() {

                @Override
                public RefreshRequest call() throws Exception {
                    return new RefreshRequest((Op) refreshable, QC.execute((Op) refreshable, CachedOpExecutor.createRootQueryIterator(executionContext), executionContext));
                }
            }, refreshable.getDuration().inMiliSec());
        }
    }

    protected synchronized void execute(RefreshRequest refreshRequest) {
        executionContext.getDataset().getLock().enterCriticalSection(true);
        List<Op> refreshedOps = new ArrayList<>();
        for (RefreshRequestSource source : refreshRequest.getSources()) {
            queryExecutionContext.getCache().put(source.getOp(), source.getResult());
            refreshedOps.add(source.getOp());
        }
        queryExecutionContext.setRefreshedOps(refreshedOps);
        QueryIterator result = QC.execute(op, initialBinding, executionContext);
        queryExecutionContext.clearRefreshedOps();
        // check if result it is not a dummy result
        processResult(result, queryExecutionContext);
        executionContext.getDataset().getLock().leaveCriticalSection();
    }

    private void processResult(QueryIterator result, QueryExecutionContext queryExecutionContext) {
        QueryIteratorCopy resultAsCopy;
        if (result instanceof QueryIteratorCopy) {
            resultAsCopy = (QueryIteratorCopy) result;
        } else {
            resultAsCopy = new QueryIteratorCopy(result, executionContext);
        }
        List<Binding> tempResult = new ArrayList<>();
        while (resultAsCopy.hasNext()) {
            Binding currentBinding = resultAsCopy.nextBinding();
            if (currentBinding.isEmpty()) {
                continue;
            };
            if (currentBinding.contains(ABORT_QUERY_VARIABLE)) {
                if (currentBinding.get(ABORT_QUERY_VARIABLE).getLiteralValue() == NodeValue.TRUE) {
                    continue;
                }
            }
            tempResult.add(currentBinding);
        }
        result.close();
        resultAsCopy.close();
        if (tempResult.size() > 0) {
            QueryIterator finalResult = new QueryIterPlainWrapper(tempResult.iterator());
            fireNewQueryResultAvailable(new NewQueryResultAvailableEvent(this, finalResult, queryExecutionContext.getRefreshedOps()));
        }
    }

    public void addNewQueryResultAvailableListener(NewQueryResultAvailableListener listener) {
        listeners.add(NewQueryResultAvailableListener.class, listener);
    }

    public void removeNewQueryResultAvailableListener(NewQueryResultAvailableListener listener) {
        listeners.remove(NewQueryResultAvailableListener.class, listener);
    }

    protected void fireNewQueryResultAvailable(NewQueryResultAvailableEvent e) {
        if (e.getResult() == null) {
            return;
        }
        Object[] temp = listeners.getListenerList();
        for (int i = 0; i < temp.length; i = i + 2) {
            if (temp[i] == NewQueryResultAvailableListener.class) {
                ((NewQueryResultAvailableListener) temp[i + 1]).newQueryResultAvailable(e);
            }
        }
    }

    @Override
    public void refreshRequested(RefreshRequestedEvent e) {
        //refreshRequestQueue.add(e.getRefreshRequest());
        execute(e.getRefreshRequest());
    }

}
