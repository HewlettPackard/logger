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
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.*;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogReaderMonitorTest {

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
    public void testLogReaderMonitor() throws Exception {
        CountDownLatch mockLogReaderLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogReaderLatch).countDown();

        ExecutorService mockLogReaderService = mock(ExecutorService.class);
        when(mockLogReaderService.awaitTermination(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(false).thenReturn(true);
        when(mockLogReaderService.isTerminated()).thenReturn(true);

        Future<Tuple<Long, String>> mockFuture = mock(Future.class);
        when(mockFuture.get()).thenReturn(new Tuple<>(1L, "dummy_log_type"));

        ExecutorCompletionService<Tuple<Long, String>> mockLogReaderECS = mock(ExecutorCompletionService.class);
        when(mockLogReaderECS.poll()).thenReturn(mockFuture).thenReturn(null);

        LogReaderMonitor logReaderMonitor = new LogReaderMonitor(mockLogReaderLatch, mockLogReaderService, mockLogReaderECS);
        logReaderMonitor.run();

        Mockito.verify(mockLogReaderLatch, Mockito.times(1)).countDown();
        Mockito.verify(mockLogReaderService, Mockito.times(2)).awaitTermination(Mockito.anyLong(), Mockito.any(TimeUnit.class));
        Mockito.verify(mockLogReaderService, Mockito.times(1)).isTerminated();
        Mockito.verify(mockLogReaderECS, Mockito.times(3)).poll();
    }

}