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
package com.github.ecqels.lang.element;

import com.github.ecqels.lang.window.Duration;
import com.github.ecqels.lang.window.WindowInfo;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.syntax.Element;

/**
 *
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email michael.jacoby@iosb.fraunhofer.de
 */
public class ElementRefreshableStream extends ElementStream {

    private final Duration duration;

    public ElementRefreshableStream(Node n, WindowInfo w, Element el, Duration duration) {
        super(n, w, el);
        this.duration = duration;
    }

    public Duration getDuration() {
        return duration;
    }
}
