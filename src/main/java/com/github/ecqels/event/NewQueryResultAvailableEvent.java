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

import java.util.ArrayList;
import java.util.EventObject;
import java.util.List;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class NewQueryResultAvailableEvent extends EventObject {

    private final QueryIterator result;
    private final List<Op> triggeredBy = new ArrayList<>();

    public NewQueryResultAvailableEvent(Object source, QueryIterator result, Op triggeredBy) {
        super(source);
        this.result = result;
        this.triggeredBy.add(triggeredBy);
    }

    public NewQueryResultAvailableEvent(Object source, QueryIterator result, List<? extends Op> triggeredBy) {
        super(source);
        this.result = result;
        this.triggeredBy.addAll(triggeredBy);
    }

    public QueryIterator getResult() {
        return result;
    }

    public List<Op> getTriggeredBy() {
        return triggeredBy;
    }

}
