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
package com.github.ecqels.example;

import com.github.ecqels.ECQELSRuntime;
import com.github.ecqels.continuous.ContinuousSelect;
import com.github.ecqels.continuous.ContinuousSelectListenerBase;
import com.github.ecqels.stream.TimedRunnableRDFStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QuerySolution;

/**
 *
 * @author Michael Jacoby
 */
public class App {

    private static final String STREAM_URI = "http://example.com/stream/1";
    private static final String SENSOR_URI = "http://example.com/sensor/1";
    private static final String HAS_VALUE_URI = "http://example.com/hasValue";

    public static void main(String[] args) throws IOException {

        ECQELSRuntime runtime = new ECQELSRuntime();
        runtime.start();
        runtime.addStream(new TimedRunnableRDFStream(runtime.getEngine(), STREAM_URI, 1) {
            private final Random random = new Random();

            @Override
            protected void execute() {
                stream(new Triple(
                        NodeFactory.createURI(SENSOR_URI),
                        NodeFactory.createURI(HAS_VALUE_URI),
                        NodeFactory.createLiteralByValue(random.nextInt(42), TypeMapper.getInstance().getTypeByClass(Integer.class))));
            }
        });
        ContinuousSelect select = runtime.registerSelect("SELECT ?s ?p ?o WHERE {  STREAM <" + STREAM_URI + "> [NOW] {?s ?p ?o .}}");
        select.addListener(new ContinuousSelectListenerBase() {
            @Override
            public void update(List<QuerySolution> result) {
                for (QuerySolution solution : result) {
                    System.out.println(solution.toString());
                }
            }
        }
        );
        System.in.read();
        runtime.shutdown();

    }
}
