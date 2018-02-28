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

package com.niara.logger.handlers;

import com.niara.logger.parsers.LogParserCounter;
import com.niara.logger.parsers.ParsingService;
import com.niara.logger.utils.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Semaphore;

public class LogReaderToLogParserHandler implements LogReaderToLogParser {

    private static final Logger logger = LoggerFactory.getLogger(LogReaderToLogParserHandler.class);

    private final String logType;
    private final Semaphore parserSemaphore;
    private final ExecutorCompletionService<LogParserCounter> parserECS;

    public LogReaderToLogParserHandler(final String logType, Semaphore parserSemaphore, ExecutorCompletionService<LogParserCounter> parserECS) {
        this.logType = logType;
        this.parserSemaphore = parserSemaphore;
        this.parserECS = parserECS;
    }

    public void start() {
        logger.info("Starting logReaderToLogParser for logType {}", logType);
    }

    public void parse(final ArrayList<String> logs) {
        if (LoggerConfig.getLogInput2Config(logType) != null) {
            try {
                parserSemaphore.acquire();
                LogParserToLogWriter logParserToLogWriter = new LogParserToLogWriterHandler(logType, LoggerConfig.getLogInput2Output(logType), com.niara.logger.apps.Logger.getLogWriterECS());
                this.parserECS.submit(new ParsingService(logType, logs, logParserToLogWriter));
            } catch (Exception e) {
                logger.error("Failed to submit logs to Parser", e);
                parserSemaphore.release();
            }
        } else {
            logger.error("Parsing config missing for log type: {}", logType);
        }
    }

    public void done() {
        logger.info("Finished logReaderToLogParser for logType {}", logType);
    }

}
