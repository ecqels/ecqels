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
package com.github.ecqels.refresh;

import com.github.ecqels.event.RefreshRequestedEvent;
import com.github.ecqels.event.RefreshRequestedListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import javax.swing.event.EventListenerList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class RefreshManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RefreshManager.class);
    protected Map<Callable<RefreshRequest>, Long> tasks;
    protected Timer timer = new Timer();
    protected ScheduledExecutorService executor;
    protected EventListenerList listeners = new EventListenerList();

    public RefreshManager() {
        tasks = new HashMap<>();
    }

    public void schedule(Callable<RefreshRequest> task, long interval) {
        tasks.put(task, interval);
    }

    public void unschedule(Callable<RefreshRequest> task) {
        tasks.remove(task);
    }

    public void stop() {
        timer.cancel();
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    public void start() {
        if (tasks.isEmpty()) {
            return;
        }
        long gcd = gcd(tasks.values().toArray(new Long[tasks.size()]));
        long lcm = lcm(tasks.values().toArray(new Long[tasks.size()]));
        executor = Executors.newScheduledThreadPool(2);
        timer.schedule(new TimerTask() {
            long timeExpired = lcm;

            @Override
            public void run() {
                RefreshRequest finalRequest = new RefreshRequest();
                // find all tasks to execute here
                timeExpired = (timeExpired % gcd) + lcm;
                Set<Callable<RefreshRequest>> toRun = tasks.entrySet().stream().filter(entry -> (timeExpired % entry.getValue()) == 0).collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getKey())).keySet();
                CompletionService<RefreshRequest> completionService = new ExecutorCompletionService<>(executor);
                toRun.stream().forEach((action) -> {
                    completionService.submit(action);
                });
                int n = toRun.size();
                for (int i = 0; i < n; ++i) {
                    RefreshRequest request;
                    try {
                        request = completionService.take().get();
                        if (request != null) {
                            finalRequest.addSource(request);
                        }
                    } catch (InterruptedException | ExecutionException ex) {
                        LOGGER.error("error executing scheduled refresh", ex);
                    }
                }
                fireRefreshRequestedListener(new RefreshRequestedEvent(this, finalRequest));
            }
        }, lcm, lcm);
    }

    public void addRefreshRequestedListener(RefreshRequestedListener listener) {
        listeners.add(RefreshRequestedListener.class, listener);
    }

    public void removeRefreshRequestedListener(RefreshRequestedListener listener) {
        listeners.remove(RefreshRequestedListener.class, listener);
    }

    protected void fireRefreshRequestedListener(RefreshRequestedEvent e) {
        Object[] temp = listeners.getListenerList();
        for (int i = 0; i < temp.length; i = i + 2) {
            if (temp[i] == RefreshRequestedListener.class) {
                ((RefreshRequestedListener) temp[i + 1]).refreshRequested(e);
            }
        }
    }

    protected long gcd(long a, long b) {
        while (b > 0) {
            long temp = b;
            b = a % b; // % is remainder
            a = temp;
        }
        return a;
    }

    protected long gcd(Long[] input) {
        long result = input[0];
        for (int i = 1; i < input.length; i++) {
            result = gcd(result, input[i]);
        }
        return result;
    }

    protected long lcm(long a, long b) {
        return a * (b / gcd(a, b));
    }

    protected long lcm(Long[] input) {
        long result = input[0];
        for (int i = 1; i < input.length; i++) {
            result = lcm(result, input[i]);
        }
        return result;
    }
}
