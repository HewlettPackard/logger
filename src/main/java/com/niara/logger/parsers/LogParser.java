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

package com.niara.logger.parsers;

import com.niara.logger.handlers.LogParserToLogWriter;
import com.niara.logger.utils.Output;
import com.niara.logger.exceptions.*;
import com.niara.logger.utils.LoggerConfig;
import com.swrve.ratelimitedlogger.RateLimitedLog;
import org.joda.time.Duration;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


public abstract class LogParser implements Callable<LogParserCounter> {

    private static final Logger slf4jLogger = LoggerFactory.getLogger(LogParser.class);

    private static RateLimitedLog logger = RateLimitedLog.withRateLimit(slf4jLogger).maxRate(LoggerConfig.getLogParsersRateLimitRate()).every(Duration.standardSeconds(LoggerConfig.getLogParsersRateLimitInterval())).build();

    protected final String inputLogType;

    protected final List<String> logs;

    protected final JSONObject parsingConfig;

    protected final LogParserToLogWriter logParserToLogWriter;

    public LogParser(final String inputLogType, final List<String> logs, LogParserToLogWriter logParserToLogWriter) {
        this.logs = logs;
        this.inputLogType = inputLogType;
        this.parsingConfig = LoggerConfig.getLogInput2Config().get(inputLogType);
        this.logParserToLogWriter = logParserToLogWriter;
    }

    public LogParserCounter parseLogs() {

        LogParserCounter parserCounter = new LogParserCounter(inputLogType, LoggerConfig.getLogInput2Output(inputLogType), logs.size());
        ArrayList<Output> parsedLogs = new ArrayList<>(LoggerConfig.getMaxLinesPerBatch());

        for (String log : logs) {
            try {
                Output parsedOutput = parse(log);
                if (parsedOutput == null) {
                    logger.error("Parsed Output is {} for log line {}", null, log);
                    parserCounter.incrementUnexpectedErrorCount();
                    continue;
                }
                parsedLogs.add(parsedOutput);
            } catch (GrokException e) {
                logger.error("GrokException: {} for log line: {}", e, log);
                parserCounter.incrementGrokErrorCount();
            } catch (UnsupportedConditionException e) {
                logger.error("UnsupportedConditionException: {} for log line: {}", e, log);
                parserCounter.incrementUnsupportedConditionCount();
            } catch (MatchDropConditionException e) {
                logger.error("MatchDropConditionException: {} for log line: {}", e, log);
                parserCounter.incrementMatchDropConditionCount();
            } catch (InvalidTimestampException e) {
                logger.error("InvalidTimestampException: {} for log line: {}", e, log);
                parserCounter.incrementInvalidTimestampErrorCount();
            } catch (Exception e) {
                logger.error("UnexpectedParsingException: {} for log line: {}", e, log);
                parserCounter.incrementUnexpectedErrorCount();
            }
        }

        logParserToLogWriter.write(parsedLogs);

        return parserCounter;

    }

    @Override
    public LogParserCounter call() {
        return parseLogs();
    }

    protected abstract Output parse(final String log) throws Exception;

}