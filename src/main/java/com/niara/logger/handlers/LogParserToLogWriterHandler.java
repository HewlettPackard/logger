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

import com.niara.logger.utils.Output;
import com.niara.logger.writers.FileWriter;
import com.niara.logger.writers.LogWriterCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ExecutorCompletionService;

public class LogParserToLogWriterHandler implements LogParserToLogWriter {

    private static final Logger logger = LoggerFactory.getLogger(LogParserToLogWriterHandler.class);

    private final String inputLogType;
    private final String outputLogType;
    private final ExecutorCompletionService<LogWriterCounter> writerECS;

    public LogParserToLogWriterHandler(final String inputLogType, final String outputLogType, ExecutorCompletionService<LogWriterCounter> writerECS) {
        this.inputLogType = inputLogType;
        this.outputLogType = outputLogType;
        this.writerECS = writerECS;
    }

    public void start() {
        logger.debug("Starting logParserToLogWriter for input {}, output {}.", inputLogType, outputLogType);
    }

    public void write(final ArrayList<Output> parsedLogs) {
        try {
            this.writerECS.submit(new FileWriter(inputLogType, outputLogType, parsedLogs));
        } catch (Exception e) {
            logger.error("Failed to submit parsed logs to writer");
        }
    }

    public void done() {
        logger.debug("Finished logParserToLogWriter for input {}, output {}.", inputLogType, outputLogType);
    }

}
