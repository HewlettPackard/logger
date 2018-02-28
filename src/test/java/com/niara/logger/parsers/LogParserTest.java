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

import com.niara.logger.exceptions.GrokException;
import com.niara.logger.exceptions.InvalidTimestampException;
import com.niara.logger.exceptions.MatchDropConditionException;
import com.niara.logger.exceptions.UnsupportedConditionException;
import com.niara.logger.handlers.LogParserToLogWriter;
import com.niara.logger.utils.GrokHandler;
import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Output;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class LogParserTest {

    @BeforeClass
    public void setUp() {
        LoggerConfig.init("TEST");
        GrokHandler.init();
    }

    @Test
    public void testParseLogsWithGrokErrorCount() {
        List<String> logs = new ArrayList<>();
        logs.add("sample log line");
        LogParserToLogWriter mockLogParserToLogWriter = mock(LogParserToLogWriter.class);
        doNothing().when(mockLogParserToLogWriter).write(Mockito.any(ArrayList.class));
        LogParser logParser = new LogParser("dummy_log_type", logs, mockLogParserToLogWriter) {
            @Override
            protected Output parse(String log) throws Exception {
                throw new GrokException("Test Grok Error");
            }
        };

        LogParserCounter logParserCounter = logParser.call();

        Assert.assertEquals(logParserCounter.getGrokErrorCount(), 1);
        Assert.assertEquals(logParserCounter.getErrorCount(), 1);
        Mockito.verify(mockLogParserToLogWriter, Mockito.times(1)).write(Mockito.any(ArrayList.class));
    }

    @Test
    public void testParseLogsWithUnsupportedConditionCount() {
        List<String> logs = new ArrayList<>();
        logs.add("sample log line");
        LogParserToLogWriter mockLogParserToLogWriter = mock(LogParserToLogWriter.class);
        doNothing().when(mockLogParserToLogWriter).write(Mockito.any(ArrayList.class));
        LogParser logParser = new LogParser("dummy_log_type", logs, mockLogParserToLogWriter) {
            @Override
            protected Output parse(String log) throws Exception {
                throw new UnsupportedConditionException("Test UnsupportedCondition Count");
            }
        };

        LogParserCounter logParserCounter = logParser.call();

        Assert.assertEquals(logParserCounter.getUnsupportedConditionCount(), 1);
        Assert.assertEquals(logParserCounter.getErrorCount(), 0);
        Mockito.verify(mockLogParserToLogWriter, Mockito.times(1)).write(Mockito.any(ArrayList.class));
    }

    @Test
    public void testParseLogsWithMatchDropConditionCount() {
        List<String> logs = new ArrayList<>();
        logs.add("sample log line");
        LogParserToLogWriter mockLogParserToLogWriter = mock(LogParserToLogWriter.class);
        doNothing().when(mockLogParserToLogWriter).write(Mockito.any(ArrayList.class));
        LogParser logParser = new LogParser("dummy_log_type", logs, mockLogParserToLogWriter) {
            @Override
            protected Output parse(String log) throws Exception {
                throw new MatchDropConditionException("Test MatchDropCondition Count");
            }
        };

        LogParserCounter logParserCounter = logParser.call();

        Assert.assertEquals(logParserCounter.getMatchDropConditionCount(), 1);
        Assert.assertEquals(logParserCounter.getErrorCount(), 0);
        Mockito.verify(mockLogParserToLogWriter, Mockito.times(1)).write(Mockito.any(ArrayList.class));
    }

    @Test
    public void testParseLogsWithInvalidTimestampErrorCount() {
        List<String> logs = new ArrayList<>();
        logs.add("sample log line");
        LogParserToLogWriter mockLogParserToLogWriter = mock(LogParserToLogWriter.class);
        doNothing().when(mockLogParserToLogWriter).write(Mockito.any(ArrayList.class));
        LogParser logParser = new LogParser("dummy_log_type", logs, mockLogParserToLogWriter) {
            @Override
            protected Output parse(String log) throws Exception {
                throw new InvalidTimestampException("Test InvalidTimestamp Error");
            }
        };

        LogParserCounter logParserCounter = logParser.call();

        Assert.assertEquals(logParserCounter.getInvalidTimestampErrorCount(), 1);
        Assert.assertEquals(logParserCounter.getErrorCount(), 1);
        Mockito.verify(mockLogParserToLogWriter, Mockito.times(1)).write(Mockito.any(ArrayList.class));
    }

    @Test
    public void testParseLogsWithUnexpectedErrorCount() {
        List<String> logs = new ArrayList<>();
        logs.add("sample log line");
        LogParserToLogWriter mockLogParserToLogWriter = mock(LogParserToLogWriter.class);
        doNothing().when(mockLogParserToLogWriter).write(Mockito.any(ArrayList.class));
        LogParser logParser = new LogParser("dummy_log_type", logs, mockLogParserToLogWriter) {
            @Override
            protected Output parse(String log) throws Exception {
                throw new Exception("Test Unexpected Parsing Error");
            }
        };

        LogParserCounter logParserCounter = logParser.call();

        Assert.assertEquals(logParserCounter.getUnexpectedErrorCount(), 1);
        Assert.assertEquals(logParserCounter.getErrorCount(), 1);
        Mockito.verify(mockLogParserToLogWriter, Mockito.times(1)).write(Mockito.any(ArrayList.class));
    }

    @Test
    public void testParseLogsWithNull() {
        List<String> logs = new ArrayList<>();
        logs.add("sample log line");
        LogParserToLogWriter mockLogParserToLogWriter = mock(LogParserToLogWriter.class);
        doNothing().when(mockLogParserToLogWriter).write(Mockito.any(ArrayList.class));
        LogParser logParser = new LogParser("dummy_log_type", logs, mockLogParserToLogWriter) {
            @Override
            protected Output parse(String log) throws Exception {
                return null;
            }
        };

        LogParserCounter logParserCounter = logParser.call();

        Assert.assertEquals(logParserCounter.getUnexpectedErrorCount(), 1);
        Assert.assertEquals(logParserCounter.getErrorCount(), 1);
        Mockito.verify(mockLogParserToLogWriter, Mockito.times(1)).write(Mockito.any(ArrayList.class));
    }

    @Test
    public void testParseLogsWithValidOutput() {
        List<String> logs = new ArrayList<>();
        logs.add("sample log line");
        LogParserToLogWriter mockLogParserToLogWriter = mock(LogParserToLogWriter.class);
        doNothing().when(mockLogParserToLogWriter).write(Mockito.any(ArrayList.class));
        LogParser logParser = new LogParser("dummy_log_type", logs, mockLogParserToLogWriter) {
            @Override
            protected Output parse(String log) throws Exception {
                return new Output("parsed log", 1);
            }
        };

        LogParserCounter logParserCounter = logParser.call();

        Assert.assertEquals(logParserCounter.getSuccessfulParsedLogCount(), 1);
        Assert.assertEquals(logParserCounter.getErrorCount(), 0);
        Mockito.verify(mockLogParserToLogWriter, Mockito.times(1)).write(Mockito.any(ArrayList.class));
    }

}