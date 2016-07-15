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
import com.github.ecqels.lang.parser.ParserECQELS;
import com.github.ecqels.query.execution.QueryExecutor;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb.base.file.FileFactory;
import org.apache.jena.tdb.base.file.FileSet;
import org.apache.jena.tdb.base.objectfile.ObjectFile;
import org.apache.jena.tdb.index.IndexFactory;
import org.apache.jena.tdb.solver.OpExecutorTDB1;
import org.apache.jena.tdb.store.nodetable.NodeTable;
import org.apache.jena.tdb.store.nodetable.NodeTableNative;
import org.apache.jena.tdb.sys.SystemTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class Engine {

    private static final Logger LOGGER = LoggerFactory.getLogger(Engine.class);

    private final NodeTable dictionary;
    private final Dataset dataset;
    private final ExecutionContext arqExecutionContext;
    private final ObjectFile dictionaryFileCache;
    private final Map<ContinuousQuery<?>, QueryExecutor> registeredQueries = new HashMap<>();
    private final ExecutorService executorPool;
    private static final ThreadGroup THREAD_GROUP_NAME = new ThreadGroup("ECQELS execution threads");

    public Engine() {
        dictionaryFileCache = FileFactory.createObjectFileMem("temp");
        dictionary = new NodeTableNative(IndexFactory.buildIndex(FileSet.mem(), SystemTDB.nodeRecordFactory), dictionaryFileCache);
        this.dataset = DatasetFactory.create();
        this.arqExecutionContext = new ExecutionContext(dataset.getContext(), dataset.asDatasetGraph().getDefaultGraph(), dataset.asDatasetGraph(), OpExecutorTDB1.OpExecFactoryTDB);
        this.executorPool = Executors.newCachedThreadPool(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(THREAD_GROUP_NAME, r);
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void shutdown() {
        try {
            registeredQueries.values().forEach(q -> q.stop());
            dataset.close();
            dictionaryFileCache.close();
            dictionary.close();
        } catch (Exception e) {
            LOGGER.debug("Could not should down correctly", e);
        }
    }

    public ExecutionContext getARQExecutionContext() {
        return arqExecutionContext;
    }

    public void addRDF(String graphUri, String data, String language) {
        addRDF(dataset.getNamedModel(graphUri), data, language);
    }

    public void addRDF(String data, String language) {
        addRDF(dataset.getDefaultModel(), data, language);
    }

    public void deleteRDF(String graphUri, String data, String language) {
        deleteRDF(dataset.getNamedModel(graphUri), data, language);
    }

    public void deleteRDF(String data, String language) {
        deleteRDF(dataset.getDefaultModel(), data, language);
    }

    public Dataset getDataset() {
        return dataset;
    }

    public Query parse(String query) {
        Query result = new Query();
        ParserECQELS parser = new ParserECQELS();
        parser.parse(result, query);
        return result;
    }

    public ContinuousSelect registerSelect(Query query, Map<String, String> variableBindings) {
        QueryExecutor queryExecutor = new QueryExecutor(this, query, variableBindings);
        ContinuousSelect result = new ContinuousSelect(query, getARQExecutionContext());
        queryExecutor.addNewQueryResultAvailableListener(result);
        registeredQueries.put(result, queryExecutor);
        queryExecutor.start();
        return result;
    }

    public void unregisterSelect(ContinuousSelect query) {
        unregister(query);
    }

    public void unregisterConstruct(ContinuousConstruct query) {
        unregister(query);
    }

    public ContinuousConstruct registerConstruct(Query query, Map<String, String> variableBindings) {
        QueryExecutor queryExecutor = new QueryExecutor(this, query, variableBindings);
        ContinuousConstruct result = new ContinuousConstruct(query, getARQExecutionContext());
        queryExecutor.addNewQueryResultAvailableListener(result);
        registeredQueries.put(result, queryExecutor);
        queryExecutor.start();
        return result;
    }

    public void sendAsync(final Node graph, final Node s, final Node p, final Node o) {
        LOGGER.debug("data received on stream " + graph + ": " + s + " " + p + " " + o);
        for (QueryExecutor query : registeredQueries.values()) {
            executorPool.submit(() -> {
                query.send(graph, s, p, o);
                return null;
            });
        }

    }

    public void send(Node graph, Node s, Node p, Node o) {
        LOGGER.debug("data received on stream " + graph + ": " + s + " " + p + " " + o);
        if (registeredQueries.size() > 1) {
            List<Callable<Void>> tasks = new ArrayList<>(registeredQueries.size());
            for (QueryExecutor query : registeredQueries.values()) {
                tasks.add((Callable<Void>) () -> {
                    query.send(graph, s, p, o);
                    return null;
                });

            }
            try {
                executorPool.invokeAll(tasks);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(Engine.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            }
        } else {
            registeredQueries.values().forEach(q -> q.send(graph, s, p, o));
        }
    }

    private void unregister(ContinuousQuery query) {
        if (!registeredQueries.containsKey(query)) {
            return;
        }
        registeredQueries.get(query).stop();
        registeredQueries.remove(query);
    }

    private void addRDF(Model model, String data, String language) {
        if (data.isEmpty()) {
            return;
        }
        Model temp = ModelFactory.createDefaultModel();
        temp.read(new ByteArrayInputStream(data.getBytes()), null, language);
        model.add(temp);
    }

    private void deleteRDF(Model model, String data, String language) {
        Model temp = ModelFactory.createDefaultModel();
        temp.read(new ByteArrayInputStream(data.getBytes()), null, language);
        model.remove(temp);
    }
}
