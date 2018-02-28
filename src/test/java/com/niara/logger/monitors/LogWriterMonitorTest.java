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

import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Tuple;
import com.niara.logger.writers.LogWriterCounter;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.*;

import static org.mockito.Mockito.*;

public class LogWriterMonitorTest {

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
    public void testLogWriterMonitor() throws Exception {
        CountDownLatch mockLogWriterLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogWriterLatch).countDown();

        ExecutorService mockLogWriterService = mock(ExecutorService.class);
        when(mockLogWriterService.awaitTermination(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(false).thenReturn(true);
        when(mockLogWriterService.isTerminated()).thenReturn(true);

        Future<LogWriterCounter> mockFuture = mock(Future.class);
        when(mockFuture.get()).thenReturn(new LogWriterCounter("dummy_input", "dummy_output", 0));

        ExecutorCompletionService<LogWriterCounter> mockLogWriterECS = mock(ExecutorCompletionService.class);
        when(mockLogWriterECS.poll()).thenReturn(mockFuture).thenReturn(null);

        LogWriterMonitor logReaderMonitor = new LogWriterMonitor(mockLogWriterLatch, mockLogWriterService, mockLogWriterECS);
        logReaderMonitor.run();

        Mockito.verify(mockLogWriterLatch, Mockito.times(1)).countDown();
        Mockito.verify(mockLogWriterService, Mockito.times(2)).awaitTermination(Mockito.anyLong(), Mockito.any(TimeUnit.class));
        Mockito.verify(mockLogWriterService, Mockito.times(1)).isTerminated();
        Mockito.verify(mockLogWriterECS, Mockito.times(3)).poll();
    }

}