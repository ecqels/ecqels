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
package com.github.ecqels;

import com.github.ecqels.continuous.ContinuousConstruct;
import com.github.ecqels.continuous.ContinuousQuery;
import com.github.ecqels.continuous.ContinuousSelect;
import com.github.ecqels.stream.RDFStream;
import com.github.ecqels.stream.RunnableRDFStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class ECQELSRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(ECQELSRuntime.class);
    private final Engine engine;
    private final ExecutorService executorService;
    private final List<RDFStream> streams = new ArrayList<>();
    private boolean running = false;

    public ECQELSRuntime() {
        engine = new Engine();
        executorService = Executors.newCachedThreadPool();
    }

    public void registerStream(RDFStream stream, String metadataGraph, String metadata) {
        engine.addRDF(metadataGraph, metadata, "N-TRIPLES");
        addStream(stream);
    }

    public void unregisterStream(RDFStream stream, String metadataGraph, String metadata) {
        engine.deleteRDF(metadataGraph, metadata);
        if (streams.contains(stream)) {
            stream.stop();
            streams.remove(stream);
        }
    }

    public Engine getEngine() {
        return engine;
    }

    public void addStream(RDFStream stream) {
        streams.add(stream);
        if (running && stream instanceof RunnableRDFStream) {
            RunnableRDFStream runnable = (RunnableRDFStream) stream;
            if (!runnable.isRunning()) {
                executorService.execute(runnable);
            };
        }
    }

    public void addStreams(List<? extends RDFStream> streams) {
        for (RDFStream stream : streams) {
            addStream(stream);
        }
    }

    public void start() {
        for (RDFStream stream : streams) {
            if (stream instanceof RunnableRDFStream) {
                RunnableRDFStream runnable = (RunnableRDFStream) stream;
                if (!runnable.isRunning()) {
                    executorService.execute(runnable);
                }
            }
        }
        running = true;
    }

    public void shutdown() {
        stop();
        engine.shutdown();
    }

    public void stop() {
        running = false;
        streams.forEach((stream) -> {
            stream.stop();
        });
        try {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            LOGGER.error("error stopping the ecqels runtime", ex);
        }
    }

    public ContinuousSelect registerSelect(String query) {
        return registerSelect(engine.parse(query));
    }

    public ContinuousSelect registerSelect(Query query) {
        return registerSelect(query, new HashMap<>());
    }

    public ContinuousSelect registerSelect(Query query, Map<String, String> variableBindings) {
        return engine.registerSelect(query, variableBindings);
    }

    public void unregisterQuery(ContinuousQuery query) {
        if (ContinuousSelect.class.isAssignableFrom(query.getClass())) {
            engine.unregisterSelect((ContinuousSelect) query);
        } else {
            engine.unregisterConstruct((ContinuousConstruct) query);
        }

    }

    public void unregisterSelect(ContinuousSelect query) {
        engine.unregisterSelect(query);
    }

    public void unregisterConstruct(ContinuousConstruct query) {
        engine.unregisterConstruct(query);
    }

    public ContinuousQuery registerQuery(String queryString) {
        Query query = engine.parse(queryString);
        if (query.isSelectType()) {
            return registerSelect(query);
        }
        if (query.isConstructType()) {
            return registerConstruct(query);
        }
        throw new IllegalArgumentException("Invalid query! Must be SELECT or CONSTRUCT!");
    }

    public ContinuousConstruct registerConstruct(String query) {
        return registerConstruct(engine.parse(query));
    }

    public ContinuousConstruct registerConstruct(Query query) {
        return registerConstruct(query, new HashMap<>());
    }

    public ContinuousConstruct registerConstruct(Query query, Map<String, String> variableBindings) {
        return engine.registerConstruct(query, variableBindings);
    }

    public void send(Quad quad) {
        send(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }

    public void send(Node graph, Triple triple) {
        send(graph, triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void send(Node graph, Node s, Node p, Node o) {
        engine.send(graph, s, p, o);
    }

    public void sendAsync(Quad quad) {
        sendAsync(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }

    public void sendAsync(Node graph, Triple triple) {
        sendAsync(graph, triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void sendAsync(Node graph, Node s, Node p, Node o) {
        engine.sendAsync(graph, s, p, o);
    }
}
