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
package com.github.ecqels.stream;

import com.github.ecqels.Engine;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public abstract class TimedRunnableRDFStream extends AbstractRDFStream implements RunnableRDFStream {

    protected boolean running = false;
    protected boolean stop = false;
    protected long sleep = 10 * 1000;

    public TimedRunnableRDFStream(Engine engine, String uri) {
        super(engine, uri);
    }

    public TimedRunnableRDFStream(Engine engine, String uri, long rate) {
        this(engine, uri);
        this.setRate(rate);
    }

    @Override
    public void stop() {
        stop = true;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRate(float rate) {
        sleep = (long) (1000 / rate);
    }

    public long getRate() {
        return sleep / 1000;
    }

    @Override
    public void run() {
        running = true;
        while (!stop) {
            execute();
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException ex) {
                Logger.getLogger(TimedRunnableRDFStream.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        stop = false;
        running = false;
    }

    protected abstract void execute();
}
