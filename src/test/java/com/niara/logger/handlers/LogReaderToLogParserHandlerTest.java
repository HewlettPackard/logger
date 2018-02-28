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
import org.json.simple.JSONObject;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Semaphore;

import static org.mockito.Mockito.*;

public class LogReaderToLogParserHandlerTest {

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
    }

    @BeforeMethod
    public void setUp() {
        Assert.assertNull(LoggerConfig.getLogInput2Config("test_log_type"));
        LoggerConfig.getLogInput2Config().put("test_log_type", new JSONObject());
    }

    @AfterMethod
    public void tearDown() {
        LoggerConfig.getLogInput2Config().remove("test_log_type");
        Assert.assertNull(LoggerConfig.getLogInput2Config("test_log_type"));
    }

    @Test
    public void testParseWithValidLogType() throws InterruptedException {
        Assert.assertNotNull(LoggerConfig.getLogInput2Config("test_log_type"));

        ExecutorCompletionService<LogParserCounter> mockECS = mock(ExecutorCompletionService.class);
        when(mockECS.submit(Mockito.any(ParsingService.class))).thenReturn(null);

        Semaphore mockSemaphore = mock(Semaphore.class);
        doNothing().when(mockSemaphore).acquire();
        doNothing().when(mockSemaphore).release();

        LogReaderToLogParser logReaderToLogParser = new LogReaderToLogParserHandler("test_log_type", mockSemaphore, mockECS);
        logReaderToLogParser.start();
        logReaderToLogParser.parse(new ArrayList<String>());
        logReaderToLogParser.done();

        Mockito.verify(mockSemaphore, Mockito.times(1)).acquire();
        Mockito.verify(mockSemaphore, Mockito.times(0)).release();
        Mockito.verify(mockECS, Mockito.times(1)).submit(Mockito.any(ParsingService.class));
    }

    @Test
    public void testParseWithInValidLogType() throws InterruptedException {
        Assert.assertNull(LoggerConfig.getLogInput2Config("dummy_log_type"));

        ExecutorCompletionService<LogParserCounter> mockECS = mock(ExecutorCompletionService.class);
        when(mockECS.submit(Mockito.any(ParsingService.class))).thenReturn(null);

        Semaphore mockSemaphore = mock(Semaphore.class);
        doNothing().when(mockSemaphore).acquire();
        doNothing().when(mockSemaphore).release();

        LogReaderToLogParser logReaderToLogParser = new LogReaderToLogParserHandler("dummy_log_type", mockSemaphore, mockECS);
        logReaderToLogParser.parse(new ArrayList<String>());

        Mockito.verify(mockSemaphore, Mockito.times(0)).acquire();
        Mockito.verify(mockSemaphore, Mockito.times(0)).release();
        Mockito.verify(mockECS, Mockito.times(0)).submit(Mockito.any(ParsingService.class));
    }

    @Test
    public void testParseWithSemaphoreThrowingException() throws InterruptedException {
        Assert.assertNotNull(LoggerConfig.getLogInput2Config("test_log_type"));

        ExecutorCompletionService<LogParserCounter> mockECS = mock(ExecutorCompletionService.class);
        when(mockECS.submit(Mockito.any(ParsingService.class))).thenReturn(null);

        Semaphore mockSemaphore = mock(Semaphore.class);
        doThrow(InterruptedException.class).when(mockSemaphore).acquire();
        doNothing().when(mockSemaphore).release();

        LogReaderToLogParser logReaderToLogParser = new LogReaderToLogParserHandler("test_log_type", mockSemaphore, mockECS);
        logReaderToLogParser.parse(new ArrayList<String>());

        Mockito.verify(mockSemaphore, Mockito.times(1)).acquire();
        Mockito.verify(mockSemaphore, Mockito.times(1)).release();
        Mockito.verify(mockECS, Mockito.times(0)).submit(Mockito.any(ParsingService.class));
    }

}