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
import java.util.Deque;
import java.util.LinkedList;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;

public class TripleWindow extends AbstractWindow {

    private final Deque<Quad> cache;
    private final long count;

    public TripleWindow(Engine engine, Node streamNode, BasicPattern pattern, long count) {
        super(engine, streamNode, pattern);
        this.count = count;
        cache = new LinkedList<>();
    }

    @Override
    public void add(final Quad quad) {
        super.add(quad);
        cache.addLast(quad);
        if (cache.size() > count) {
            datasetGraph.getLock().enterCriticalSection(false);
            Quad toDelete = cache.pollFirst();
            datasetGraph.delete(toDelete);
            cache.stream().filter(e -> e.equals(toDelete)).forEach(e -> datasetGraph.add(e));
            datasetGraph.getLock().leaveCriticalSection();
        }
//        QueryIterator result = evaluate();
//        QueryIteratorCopy temp = new QueryIteratorCopy(result);
//        fireDataChanged(temp.copy());
    }

    public void stop() {
        super.stop();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TripleWindow)) {
            return false;
        }
        TripleWindow window = (TripleWindow) obj;
        return window.count == count;
    }

    public Window clone() {
        return new TripleWindow(engine, streamNode, pattern, count);
    }
}
