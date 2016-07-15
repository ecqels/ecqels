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

import com.github.ecqels.event.DataChangedListener;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;

public interface Window {

    public void add(final Quad quad);

    public void stop();

    public DatasetGraph getDatasetGraph();

    public void addDataChangedListener(DataChangedListener listener);

    public void removeDataChangedListener(DataChangedListener listener);

    public QueryIterator evaluate(QueryIterator input, ExecutionContext execCxt);

    public Node getStreamNode();

    public BasicPattern getPattern();

    @Override
    public boolean equals(Object obj);

    public Window clone();
}
