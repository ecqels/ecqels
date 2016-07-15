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
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;

public class All extends AbstractWindow {

    public All(Engine engine, Node streamNode, BasicPattern pattern) {
        super(engine, streamNode, pattern);
    }

    @Override
    public void add(final Quad quad) {
        super.add(quad);
        fireDataChanged(evaluate());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof All)) {
            return false;
        }
        return true;
    }

    @Override
    public Window clone() {
        return new All(engine, streamNode, pattern);
    }

}
