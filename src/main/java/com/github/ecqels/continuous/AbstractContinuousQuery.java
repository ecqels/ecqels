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
package com.github.ecqels.continuous;

import com.github.ecqels.event.NewQueryResultAvailableEvent;
import com.github.ecqels.query.iterator.QueryIteratorCopy;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class AbstractContinuousQuery<T extends ContinuousListener> implements ContinuousQuery<T> {

    protected Query query;
    protected List<T> listeners = new ArrayList<>();
    protected final ExecutionContext executionContext;

    public AbstractContinuousQuery(Query query, ExecutionContext executionContext) {
        this.query = query;
        this.executionContext = executionContext;
    }

    @Override
    public void newQueryResultAvailable(NewQueryResultAvailableEvent e) {
        fireUpdate(e.getResult());
    }

    public void addListener(T listener) {
        listeners.add(listener);
    }

    public void removeListener(T listener) {
        listeners.remove(listener);
    }

    @Override
    public Query getQuery() {
        return query;
    }

    protected void fireUpdate(QueryIterator result) {
        QueryIteratorCopy iteratorCopy = null;
        if (listeners.size() > 1) {
            iteratorCopy = new QueryIteratorCopy(result, executionContext);
        }
        for (T listener : listeners) {
            listener.update(iteratorCopy == null ? result : iteratorCopy.copy());
            //listener.update(QueryIter.materialize(result));
        }
        if (listeners.size() > 1) {
            iteratorCopy.close();
        }
    }
}
