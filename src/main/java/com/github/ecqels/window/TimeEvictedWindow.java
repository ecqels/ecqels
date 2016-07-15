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
import com.github.ecqels.lang.window.Duration;
import com.github.ecqels.query.iterator.QueryIteratorCopy;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.QueryIterator;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class TimeEvictedWindow extends AbstractWindow {

    protected final Duration size;
    protected Timer timer;

    public TimeEvictedWindow(Engine engine, Node streamNode, BasicPattern pattern, Duration size) {
        super(engine, streamNode, pattern);
        this.size = size;
        timer = new Timer("window eviction timer", true);
    }

    @Override
    public void add(final Quad quad) {
        if (timer != null) {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {

                    try {
                        datasetGraph.getLock().enterCriticalSection(false);
                        datasetGraph.delete(quad);
                        datasetGraph.getLock().leaveCriticalSection();

                        QueryIterator temp = evaluate();

                        datasetGraph.getLock().enterCriticalSection(true);
                        QueryIteratorCopy result = new QueryIteratorCopy(temp, engine.getARQExecutionContext());
                        result.close();
                        datasetGraph.getLock().leaveCriticalSection();

                        //fireDataChanged(result.copy());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, size.inMiliSec());
            super.add(quad);
        }
    }

    @Override
    public void stop() {
        super.stop();
        timer.cancel();
        timer = null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TimeEvictedWindow)) {
            return false;
        }
        TimeEvictedWindow window = (TimeEvictedWindow) obj;
        return window.size.equals(size);
    }

    @Override
    public Window clone() {
        return new TimeEvictedWindow(engine, streamNode, pattern, size);
    }

}
