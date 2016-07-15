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
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class TumblingWindow extends ScheduledRefreshableWindow {

    private static final Logger LOGGER = LoggerFactory.getLogger(TumblingWindow.class);
    protected final Duration size;

    public TumblingWindow(Engine engine, Node streamNode, BasicPattern pattern, Duration size) {
        super(engine, streamNode, pattern);
        if (size == null || size.inNanoSec() <= 0) {
            throw new IllegalArgumentException("window size must be non-null and > 0");
        }
        this.size = size;
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public long getRefreshInterval() {
        return size.inMiliSec();
    }

    @Override
    public void purgeAfterExecution() {
        datasetGraph.deleteAny(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TumblingWindow)) {
            return false;
        }
        TumblingWindow window = (TumblingWindow) obj;
        return window.size.equals(size);
    }

    @Override
    public Window clone() {
        return new TumblingWindow(engine, streamNode, pattern, size);
    }
}
