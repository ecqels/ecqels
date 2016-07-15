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
package com.github.ecqels.stream;

import com.github.ecqels.ECQELSRuntime;
import com.github.ecqels.continuous.ContinuousConstructListenerBase;
import com.github.ecqels.util.Utils;
import java.util.List;
import org.apache.jena.graph.Triple;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class FeedbackStream extends ContinuousConstructListenerBase implements RDFStream {

    private boolean stop = false;
    private final String uri;
    private final ECQELSRuntime engine;

    public FeedbackStream(ECQELSRuntime engine, String uri) {
        this.engine = engine;
        this.uri = uri;
    }

    @Override
    public void stop() {
        stop = true;
    }

    @Override
    public void update(List<List<Triple>> graph) {
        if (!stop) {
            graph.stream().forEach(triples -> {
                triples.forEach(triple -> stream(triple));
            });
        }
    }

    @Override
    public String getURI() {
        return uri;
    }

    @Override
    public void stream(Triple t) {
        engine.send(Utils.toNode(uri), t.getSubject(), t.getPredicate(), t.getObject());
    }

}
