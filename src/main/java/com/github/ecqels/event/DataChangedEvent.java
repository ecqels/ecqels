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
package com.github.ecqels.event;

import com.github.ecqels.window.Window;
import java.util.EventObject;
import org.apache.jena.sparql.engine.QueryIterator;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class DataChangedEvent extends EventObject {

    private final Window window;
    private final QueryIterator result;

    public DataChangedEvent(Object source, Window window, QueryIterator result) {
        super(source);
        this.window = window;
        this.result = result;
    }

    /**
     * @return the window
     */
    public Window getWindow() {
        return window;
    }

    /**
     * @return the result
     */
    public QueryIterator getResult() {
        return result;
    }

}
