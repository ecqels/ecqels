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
import com.github.ecqels.lang.window.WindowInfo;
import static com.github.ecqels.lang.window.WindowInfo.WindowType.SLIDING;
import com.github.ecqels.refresh.RefreshManager;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class WindowFactory {

    public static Window createWindow(Engine engine, RefreshManager refreshManager, Node streamNode, BasicPattern pattern, WindowInfo info) {
        switch (info.getType()) {
            case NOW:
                return new Now(engine, streamNode, pattern);
            case ALL:
                return new All(engine, streamNode, pattern);
            case TRIPLES:
                return new TripleWindow(engine, streamNode, pattern, info.getTriples());
            case TUMBLING:
                return new TumblingWindow(engine, streamNode, pattern, info.getSize());
            case SLIDING:
                return (info.getSlide() == null || info.getSlide().inNanoSec() <= 0)
                        ? new TimeEvictedWindow(engine, streamNode, pattern, info.getSize())
                        //? new SlidingWindow(engine, streamNode, pattern, info.getSize(), info.getSize())
                        : new SlidingWindow(engine, streamNode, pattern, info.getSize(), info.getSlide());
            default:
                throw new IllegalStateException();
        }
    }

    private WindowFactory() {

    }
}
