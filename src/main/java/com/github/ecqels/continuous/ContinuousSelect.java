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
package com.github.ecqels.continuous;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.engine.ExecutionContext;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class ContinuousSelect extends AbstractContinuousQuery<ContinuousSelectListener> {

    public ContinuousSelect(Query query, ExecutionContext executionContext) {
        super(query, executionContext);
    }

}
