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

import java.util.ArrayList;
import java.util.List;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingUtils;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.syntax.Template;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class ContinuousConstructListenerBase implements ContinuousConstructListener {

    protected Template template;

    @Override
    public void update(List<List<Triple>> triples) {

    }

    @Override
    public void setTemplate(Template template) {
        this.template = template;
    }

    @Override
    public void update(QueryIterator result) {
        List<List<Triple>> temp = new ArrayList<>();
        while (result.hasNext()) {
            Binding binding = result.next();
            List<Triple> triples = new ArrayList<>();
            for (Triple triple : template.getTriples()) {
                BindingUtils r = new BindingUtils();
                triples.add(TemplateLib.subst(triple, binding, null));
            }
            temp.add(triples);
        }
        update(temp);
    }

}
