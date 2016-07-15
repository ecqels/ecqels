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
package com.github.ecqels.op;

import com.github.ecqels.lang.op.OpRefreshableGraph;
import com.github.ecqels.lang.op.OpRefreshableService;
import com.github.ecqels.lang.op.OpRefreshableStream;
import com.github.ecqels.lang.op.OpStream;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpExt;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class ECQELSOpVisitorBase extends OpVisitorBase {

    @Override
    public void visit(OpExt opExt) {
        if (opExt instanceof OpStream) {
            visit((OpStream) opExt);
        } else if (opExt instanceof OpRefreshableStream) {
            visit((OpRefreshableStream) opExt);
        } else if (opExt instanceof OpRefreshableService) {
            visit((OpRefreshableService) opExt);
        } else if (opExt instanceof OpRefreshableGraph) {
            visit((OpRefreshableGraph) opExt);
        } else {
            super.visit(opExt);
        }
    }

    public void visit(OpStream opStream) {

    }

    public void visit(OpRefreshableGraph opGraph) {

    }

    public void visit(OpRefreshableService opService) {

    }

    public void visit(OpRefreshableStream opStream) {

    }
}
