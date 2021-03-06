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

import java.io.Reader;
import java.io.StringReader;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.Syntax;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.lang.SPARQLParser;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParserECQELS extends SPARQLParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParserECQELS.class);
    public static final Syntax syntaxCQELS_01 = Syntax.make("http://deri.org/2011/05/query/CQELS_01");

    private interface Action {

        void exec(ECQELSParser parser) throws Exception;
    }

    @Override
    protected Query parse$(final Query query, String queryString) {
        query.setSyntax(syntaxCQELS_01);

        Action action = new Action() {
            public void exec(ECQELSParser parser) throws Exception {
                parser.QueryUnit();
            }
        };

        perform(query, queryString, action);
        validateParsedQuery(query);
        return query;
    }

    public static Element parseElement(String string) {
        final Query query = new Query();
        Action action = new Action() {
            public void exec(ECQELSParser parser) throws Exception {
                Element el = parser.GroupGraphPattern();
                query.setQueryPattern(el);
            }
        };
        perform(query, string, action);
        return query.getQueryPattern();
    }

    public static Template parseTemplate(String string) {
        final Query query = new Query();
        Action action = new Action() {
            public void exec(ECQELSParser parser) throws Exception {
                Template t = parser.ConstructTemplate();
                query.setConstructTemplate(t);
            }
        };
        perform(query, string, action);
        return query.getConstructTemplate();
    }

    // All throwable handling.
    private static void perform(Query query, String string, Action action) {
        Reader in = new StringReader(string);
        ECQELSParser parser = new ECQELSParser(in);

        try {
            query.setStrict(true);
            parser.setQuery(query);
            action.exec(parser);
        } catch (ParseException ex) {
            throw new QueryParseException(ex.getMessage(),
                    ex.currentToken.beginLine,
                    ex.currentToken.beginColumn
            );
        } catch (TokenMgrError tErr) {
            // Last valid token : not the same as token error message - but this should not happen
            int col = parser.token.endColumn;
            int line = parser.token.endLine;
            throw new QueryParseException(tErr.getMessage(), line, col);
        } catch (QueryException ex) {
            throw ex;
        } catch (JenaException ex) {
            throw new QueryException(ex.getMessage(), ex);
        } catch (Error err) {
            // The token stream can throw errors.
            throw new QueryParseException(err.getMessage(), err, -1, -1);
        } catch (Throwable th) {
            LOGGER.warn("unexpected throwable: ", th);
            throw new QueryException(th.getMessage(), th);
        }
    }
}
/*
 * (c) Copyright 2005, 2006, 2007, 2008, 2009 Hewlett-Packard Development Company, LP
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
