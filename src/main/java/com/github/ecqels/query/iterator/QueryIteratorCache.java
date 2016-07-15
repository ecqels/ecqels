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
package com.github.ecqels.query.iterator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;

/**
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email michael.jacoby@iosb.fraunhofer.de
 */
public class QueryIteratorCache<T> {

    private final Map<T, QueryIteratorCopy> cache = new ConcurrentHashMap<>();
    private final ExecutionContext context;

    public QueryIteratorCache(ExecutionContext context) {
        this.context = context;
    }

    public boolean containsKey(T key) {
        return cache.containsKey(key);
    }

    public QueryIterator put(T key, QueryIterator value) {
        if (cache.containsKey(key)) {
            cache.get(key).close();
        }
        cache.put(key, new QueryIteratorCopy(value, context));
        return cache.get(key);
    }

    public QueryIterator get(T key) {
        if (containsKey(key)) {
            QueryIteratorCopy iterator = cache.get(key);
            return iterator.copy();
        }
        return null;
    }

    public QueryIterator remove(T key) {
        return cache.remove(key);
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public int size() {
        return cache.size();
    }

    @Override
    public int hashCode() {
        return cache.hashCode();
    }

    public void flush() {
        cache.clear();
    }

    @Override
    public boolean equals(Object o) {
        return cache.equals(o);
    }
}
