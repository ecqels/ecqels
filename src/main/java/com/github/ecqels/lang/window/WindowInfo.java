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
package com.github.ecqels.lang.window;

/**
 *
 * @author Michael Jacoby <michael.jacoby@iosb.fraunhofer.de>
 */
public class WindowInfo {

    /**
     * @return the type
     */
    public WindowType getType() {
        return type;
    }

    /**
     * @return the triples
     */
    public long getTriples() {
        return triples;
    }

    public enum WindowType {

        NOW,
        ALL,
        TRIPLES,
        SLIDING,
        TUMBLING
    }

    private final WindowType type;
    private Duration size;
    private Duration slide;
    private long triples;
    private boolean tumbling;

    public WindowInfo(WindowType type) {
        if (!type.equals(WindowType.NOW) && !type.equals(WindowType.ALL)) {
            throw new IllegalArgumentException("parameterless constructor only allowed with [NOW] or [ALL] windows");
        }
        this.type = type;
    }

    public WindowInfo(long triples) {
        this.type = WindowType.TRIPLES;
        this.triples = triples;
    }

    public WindowInfo(Duration size, Duration slide, boolean tumbling) {
        this.type = tumbling ? WindowType.TUMBLING : WindowType.SLIDING;
        this.size = size;
        this.slide = slide;
        this.tumbling = tumbling;
    }

    public boolean isScheduledRefreshable() {
        return ((type == WindowType.SLIDING && slide != null) || type == WindowType.TUMBLING);
    }

    public Duration getScheduleInterval() {
        if (type == WindowType.SLIDING) {
            return slide;
        }
        if (type == WindowType.TUMBLING) {
            return size;
        }
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof WindowInfo)) {
            return false;
        }
        WindowInfo info = (WindowInfo) obj;
        return info.type.equals(type)
                && (info.size == size || info.size.equals(size))
                && info.tumbling == tumbling
                && (info.slide == slide || info.slide.equals(slide))
                && info.triples == triples;
    }

    /**
     * @return the size
     */
    public Duration getSize() {
        return size;
    }

    /**
     * @return the slide
     */
    public Duration getSlide() {
        return slide;
    }

    /**
     * @return the isTumbling
     */
    public boolean isTumbling() {
        return tumbling;
    }

    @Override
    public String toString() {
        switch (type) {
            case NOW:
                return "NOW";
            case ALL:
                return "ALL";
            case TRIPLES:
                return "TRIPLES " + triples;
            case SLIDING:
                return "RANGE " + size + " SLIDE " + slide;
            case TUMBLING:
                return "RANGE " + size + " TUMBLING";
        }
        return "";
    }
}
