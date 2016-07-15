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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;

public class SlidingWindow extends ScheduledRefreshableWindow {

    protected Duration size;
    protected Duration slide;
    protected long windowStart;
    protected final ConcurrentMap<Long, Quad> cache;

    public SlidingWindow(Engine engine, Node streamNode, BasicPattern pattern, Duration size, Duration slide) {
        super(engine, streamNode, pattern);
        if (size == null || size.inMiliSec() <= 0) {
            throw new IllegalArgumentException("window size must be non-null and > 0");
        }
        if (slide == null || slide.inMiliSec() <= 0) {
            throw new IllegalArgumentException("window slide must be non-null and > 0");
        }
        this.size = size;
        this.slide = slide;
        //cache = new TreeMap<>();
        cache = new ConcurrentHashMap<>();
        windowStart = System.nanoTime();
    }

    @Override
    public void add(final Quad quad) {
        // only add if in curretn window
        long timestamp = System.nanoTime();
        //   if (timestamp <= windowStart + size.inNanoSec()) {
        cache.put(timestamp, quad);
        super.add(quad);
        // }
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public long getRefreshInterval() {
        return slide.inMiliSec();
    }

    @Override
    public void purgeBeforeExecution() {
        try {
            synchronized (this) {
                long minTimestamp = System.nanoTime() - size.inNanoSec();
                for (Map.Entry<Long, Quad> entry : cache.entrySet()) {
                    if (entry.getKey() < minTimestamp) {
                        cache.remove(entry.getKey());
                        datasetGraph.delete(entry.getValue());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("error purging window" + e);
        }
//        SortedMap<Long, Quad> toRemove = cache.headMap(minTimestamp);
//        toRemove.keySet().forEach(key -> cache.remove(key));
//        toRemove.values().forEach(quad -> datasetGraph.delete(quad));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SlidingWindow)) {
            return false;
        }
        SlidingWindow window = (SlidingWindow) obj;
        return window.size.equals(size) && window.slide.equals(slide);
    }

    @Override
    public Window clone() {
        return new SlidingWindow(engine, streamNode, pattern, size, slide);
    }

}
