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

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Duration {

    protected static final Pattern PATTERN_ELEMENT = Pattern.compile("(\\d+)\\s*(d|h|m|s|ms|ns)");
    protected static final Pattern PATTERN_SUBELEMENT = Pattern.compile("\\d+\\s*(d|h|m|s|ms|ns)");
    protected static final Pattern PATTERN_ALL = Pattern.compile("(\\d+\\s*(d|h|m|s|ms|ns)\\s*)+");
    protected long nanoTime = 0;
    protected String source;

    public Duration(String source) {
        this.source = source;
        parse();
    }

    private void parse() {
        if (!source.matches(PATTERN_ALL.pattern())) {
            throw new IllegalArgumentException("Duration does not match patter '" + PATTERN_ALL.pattern() + "'");
        }
        Matcher matcher = PATTERN_ELEMENT.matcher(source);
        while (matcher.find()) {
            nanoTime += TimeUnit.NANOSECONDS.convert(Long.parseLong(matcher.group(1)), parseTimeUnit(matcher.group(2)));
        }
    }

    private TimeUnit parseTimeUnit(String unit) {
        switch (unit) {
            case "d":
                return TimeUnit.DAYS;
            case "h":
                return TimeUnit.HOURS;
            case "m":
                return TimeUnit.MINUTES;
            case "s":
                return TimeUnit.SECONDS;
            case "ms":
                return TimeUnit.MILLISECONDS;
            case "ns":
                return TimeUnit.NANOSECONDS;
        }
        throw new IllegalArgumentException("time unit '" + unit + "' is not valid, only (d|h|m|s|ms|ns)");
    }

    public long inNanoSec() {
        return nanoTime;
    }

    public long inMiliSec() {
        return inNanoSec() / (long) 1E6;
    }

    @Override
    public String toString() {
        return source;
    }
}
