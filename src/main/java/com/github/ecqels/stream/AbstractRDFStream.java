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

import com.github.ecqels.Engine;
import com.github.ecqels.util.Utils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * @author Danh Le Phuoc
 * @organization DERI Galway, NUIG, Ireland www.deri.ie
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email danh.lephuoc@deri.org
 * @email michael.jacoby@iosb.fraunhofer.de
 */
public abstract class AbstractRDFStream implements RDFStream {

    protected Node streamURI;
    protected Engine engine;

    public AbstractRDFStream(Engine engine, String uri) {
        streamURI = Utils.toNode(uri);
        this.engine = engine;
    }

    @Override
    public void stream(Triple t) {
        engine.send(streamURI, t.getSubject(), t.getPredicate(), t.getObject());
    }

    @Override
    public String getURI() {
        return streamURI.getURI();
    }
}
