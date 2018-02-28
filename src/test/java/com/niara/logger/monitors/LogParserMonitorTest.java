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

package com.niara.logger.monitors;

import com.niara.logger.parsers.LogParserCounter;
import com.niara.logger.utils.LoggerConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.*;

import static org.mockito.Mockito.*;

public class LogParserMonitorTest {

    private long statsInterval;

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
    }

    @BeforeMethod
    public void setUp() {
        statsInterval = LoggerConfig.getStatsInterval();
        LoggerConfig.setStatsInterval("0");
        Assert.assertEquals(LoggerConfig.getStatsInterval(), 0L);
    }

    @AfterMethod
    public void tearDown() {
        LoggerConfig.setStatsInterval(String.valueOf(statsInterval));
        Assert.assertEquals(LoggerConfig.getStatsInterval(), statsInterval);
    }

    @Test
    public void testLogParserMonitor() throws Exception {
        Semaphore mockLogParserSemaphore = mock(Semaphore.class);
        doNothing().when(mockLogParserSemaphore).release();

        CountDownLatch mockLogParserLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogParserLatch).countDown();

        ExecutorService mockLogParserService = mock(ExecutorService.class);
        when(mockLogParserService.awaitTermination(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(false).thenReturn(true);
        when(mockLogParserService.isTerminated()).thenReturn(true);

        Future<LogParserCounter> mockFuture = mock(Future.class);
        when(mockFuture.get()).thenReturn(new LogParserCounter("dummy_input", "dummy_output", 0));

        ExecutorCompletionService<LogParserCounter> mockLogParserECS = mock(ExecutorCompletionService.class);
        when(mockLogParserECS.poll()).thenReturn(mockFuture).thenReturn(null);

        LogParserMonitor logReaderMonitor = new LogParserMonitor(mockLogParserSemaphore, mockLogParserLatch, mockLogParserService, mockLogParserECS);
        logReaderMonitor.run();

        Mockito.verify(mockLogParserSemaphore, Mockito.times(1)).release();
        Mockito.verify(mockLogParserLatch, Mockito.times(1)).countDown();
        Mockito.verify(mockLogParserService, Mockito.times(2)).awaitTermination(Mockito.anyLong(), Mockito.any(TimeUnit.class));
        Mockito.verify(mockLogParserService, Mockito.times(1)).isTerminated();
        Mockito.verify(mockLogParserECS, Mockito.times(3)).poll();
    }

}