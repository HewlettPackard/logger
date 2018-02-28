/*
* (C) Copyright [2018] Hewlett Packard Enterprise Development LP.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.niara.logger.readers;

import com.niara.logger.apps.Logger;
import com.niara.logger.handlers.LogReaderToLogParser;
import com.niara.logger.handlers.LogReaderToLogParserHandler;
import com.niara.logger.utils.Tuple;

import java.util.concurrent.Callable;


/**
 * Read a log file, and convert into a list of log lines.
 * Currently, log per line is supported. If multi-line or other type will be
 * supported, then refactor this into a class hierarchy.
 */
public abstract class LogReader implements Callable<Tuple<Long, String>> {

    protected final String logType;
    protected final LogReaderToLogParser logReaderToLogParser;

    public LogReader(final String logType) {
        this.logType = logType;
        this.logReaderToLogParser = new LogReaderToLogParserHandler(logType, Logger.getLogParserSemaphore(), Logger.getLogParserECS());
    }

    protected abstract Tuple<Long, String> read() throws Exception;

    @Override
    public Tuple<Long, String> call() throws Exception {

        return read();

    }
}
