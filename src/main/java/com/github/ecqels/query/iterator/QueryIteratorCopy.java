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
package com.github.ecqels.query.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.serializer.SerializationContext;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class QueryIteratorCopy extends QueryIter {

    List<Binding> elements = new ArrayList<>();
    QueryIterator iterator;

    public QueryIteratorCopy(QueryIterator qIter, ExecutionContext execContext) {
        super(execContext);
        synchronized (this) {
            for (; qIter.hasNext();) {
                elements.add(qIter.nextBinding());
            }
            qIter.close();
            iterator = copy();
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        return iterator.nextBinding();
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.print("QueryIteratorCopy");
        out.incIndent();
        out.decIndent();
    }

    public List<Binding> elements() {
        return Collections.unmodifiableList(elements);
    }

    public QueryIterator copy() {
        return new QueryIterPlainWrapper(elements.iterator());
    }

    @Override
    protected void closeIterator() {
        iterator.close();
    }

    @Override
    protected void requestCancel() {
        iterator.cancel();
    }

    @Override
    protected boolean hasNextBinding() {
        return iterator.hasNext();
    }
}
