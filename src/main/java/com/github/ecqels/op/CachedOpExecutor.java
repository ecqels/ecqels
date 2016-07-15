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
package com.github.ecqels.op;

import com.github.ecqels.lang.op.OpStream;
import com.github.ecqels.query.execution.QueryExecutionContext;
import com.github.ecqels.query.iterator.QueryIteratorCopy;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import static org.apache.jena.sparql.engine.main.OpExecutor.createRootQueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.tdb.solver.OpExecutorTDB1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class CachedOpExecutor extends OpExecutorTDB1 {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachedOpExecutor.class);

    public static final OpExecutorFactory CQELSOpExecFactory = new OpExecutorFactory() {
        @Override
        public OpExecutor create(ExecutionContext execCxt) {
            return new CachedOpExecutor(execCxt);
        }
    };

    private static OpExecutor createOpExecutor(ExecutionContext execCxt) {
        OpExecutorFactory factory = execCxt.getExecutor();
        if (factory == null) {
            factory = CQELSOpExecFactory;
        }
        if (factory == null) {
            return new CachedOpExecutor(execCxt);
        }
        return factory.create(execCxt);
    }

    static QueryIterator execute(Op op, ExecutionContext execCxt) {
        return execute(op, createRootQueryIterator(execCxt), execCxt);
    }

    static QueryIterator execute(Op op, QueryIterator qIter, ExecutionContext execCxt) {
        OpExecutor exec = createOpExecutor(execCxt);
        QueryIterator q = exec.executeOp(op, qIter);
        return q;
    }

    protected CachedOpExecutor(ExecutionContext execCxt) {
        super(execCxt);
    }

    @Override
    public QueryIterator executeOp(Op op, QueryIterator input) {
        long start = System.currentTimeMillis();
        QueryIterator resultToReturn = null;
        String executeType = "[unknown]";
        if (execCxt.getContext().isDefined(QueryExecutionContext.SYMBOL)) {
            QueryExecutionContext queryContext = (QueryExecutionContext) execCxt.getContext().get(QueryExecutionContext.SYMBOL);
            QueryIterator result = null;
            // force refresh occures with Stream, RefreshableStream, RefreshableGraph, RefreshableService
            if (queryContext.isForceRefresh(op)) {
                executeType = "[execute]";
                result = super.executeOp(op, input);
            } else if (queryContext.isRefreshed(op)) {
                executeType = "[cache]";
                input.close();
                result = queryContext.getCache().get(op);
            } else if (queryContext.isAffectedFromRefresh(op)) {
                executeType = "[execute]";
                result = super.executeOp(op, input);
                //if (queryContext.isCacheable(op)) {
                //    result = queryContext.getCache().put(op, result);
                //}
            } else if (queryContext.isCacheable(op)) {
                if (queryContext.getCache().containsKey(op)) {
                    executeType = "[cache]";
                    input.close();
                } else {
                    executeType = "[execute]";
                }
//                result = queryContext.getCache().put(op,
//                        queryContext.getCache().containsKey(op)
//                                ? queryContext.getCache().get(op)
//                                : super.executeOp(op, input));
                result = queryContext.getCache().containsKey(op)
                        ? queryContext.getCache().get(op)
                        : queryContext.getCache().put(op, super.executeOp(op, input));

            } else if (op instanceof OpStream && queryContext.getCache().containsKey(op)) {
                //always fetch streams from cache as they are resposnible theirselves to refresh themselves!
                executeType = "[cache]";
                result = queryContext.getCache().get(op);
            } else {
                executeType = "[execute]";
                result = super.executeOp(op, input);
            }
            //resultToReturn = result;
            resultToReturn = printIntermediateResult(op, result);
        } else {
            resultToReturn = printIntermediateResult(op, super.executeOp(op, input));
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execution type: " + executeType + "executing took " + (System.currentTimeMillis() - start) + "ms for op \n" + op);
        }
        return resultToReturn;
    }

    private QueryIterator printIntermediateResult(Op op, QueryIterator iterator) {
        // return iterator;
        QueryIteratorCopy copy;
        if (iterator instanceof QueryIteratorCopy) {
            copy = (QueryIteratorCopy) iterator;
        } else {
            copy = new QueryIteratorCopy(iterator, execCxt);
        }

        if (LOGGER.isTraceEnabled()) {
            QueryIterator temp = copy.copy();
            LOGGER.trace("result for op " + op);
            while (temp.hasNext()) {
                LOGGER.trace("-  " + temp.next() + "\n");
            }
        }
        QueryIterator result = copy.copy();
        copy.close();
        return result;
    }
}
