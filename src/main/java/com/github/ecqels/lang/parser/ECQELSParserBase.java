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
package com.github.ecqels.lang.parser;

import com.github.ecqels.lang.window.Duration;
import org.apache.jena.sparql.lang.SPARQLParserBase;

/**
 *
 * @author micha
 */
public class ECQELSParserBase extends SPARQLParserBase {

    public Duration getDuration(String str) {
        return new Duration(str);
    }

}
