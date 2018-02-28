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

package com.niara.logger.utils;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;


public class LoggerConfigTest {

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
    }

    @Test
    public void testGetLogInput2Output() {
        /*
            input2output:
            log_apache_access -> accesslog
            ...
        */
        Map<String, String> input2output = LoggerConfig.getLogInput2Output();
        Assert.assertEquals("accesslog", input2output.get("log_apache_access"));
        Assert.assertEquals(null, input2output.get("invalid_log"));
    }

    @Test
    public void testGetStatsInterval() {
        Assert.assertEquals(LoggerConfig.getStatsInterval(), 1 * 60 * 1000);
    }

    @Test
    public void testGetMaxLinesPerBatch() {
        Assert.assertEquals(LoggerConfig.getMaxLinesPerBatch(), 10000);
    }

    @Test
    public void testGetTimestampDefault() {
        Assert.assertEquals(LoggerConfig.getTimestampDefault(), 0);
    }

    @Test
    public void testGetTimestampKeys() {
        Assert.assertEquals(Arrays.asList(LoggerConfig.getTimestampKeys()), Arrays.asList("timestamp", "data timestamp", "start"));
    }

    @Test
    public void testGetLogListenerInputDirectory() {
        Assert.assertEquals(LoggerConfig.getLogListenerInputDirectory(), "/dev/shm/logger-in");
    }

    @Test
    public void testGetLogWriterOutputDirectory() {
        Assert.assertEquals(LoggerConfig.getLogWriterOutputDirectory(), "/dev/shm/logtrove-out");
    }

    @Test
    public void testGetLogListenerPollInterval() {
        Assert.assertEquals(LoggerConfig.getLogListenerPollInterval(), 10);
    }

    @Test
    public void testGetLogListenerPollUnit() {
        Assert.assertEquals(LoggerConfig.getLogListenerPollUnit(), "milliseconds");
    }

    @Test
    public void testGetLogReaderMonitorPollInterval() {
        Assert.assertEquals(LoggerConfig.getLogReaderMonitorPollInterval(), 10);
    }

    @Test
    public void testGetLogReaderMonitorPollUnit() {
        Assert.assertEquals(LoggerConfig.getLogReaderMonitorPollUnit(), "milliseconds");
    }

    @Test
    public void testGetLogParserMonitorPollInterval() {
        Assert.assertEquals(LoggerConfig.getLogParserMonitorPollInterval(), 10);
    }

    @Test
    public void testGetLogParserMonitorPollUnit() {
        Assert.assertEquals(LoggerConfig.getLogParserMonitorPollUnit(), "milliseconds");
    }

    @Test
    public void testGetLogWriterMonitorPollInterval() {
        Assert.assertEquals(LoggerConfig.getLogWriterMonitorPollInterval(), 10);
    }

    @Test
    public void testGetLogWriterMonitorPollUnit() {
        Assert.assertEquals(LoggerConfig.getLogWriterMonitorPollUnit(), "milliseconds");
    }

    @Test
    public void testGetLogReadersPool() {
        Assert.assertEquals(LoggerConfig.getLogReadersPool(), 10);
    }

    @Test
    public void testGetLogParsersPool() {
        Assert.assertEquals(LoggerConfig.getLogParsersPool(), 20);
    }

    @Test
    public void testGetLogParsersQueueLength() {
        Assert.assertEquals(LoggerConfig.getLogParsersQueueLength(), 20);
    }

    @Test
    public void testGetParserSemaphoreMonitorPollInterval() {
        Assert.assertEquals(LoggerConfig.getLogParserSemaphoreMonitorPollInterval(), 5000);
    }

    @Test
    public void testGetParserSemaphoreMonitorPollUnit() {
        Assert.assertEquals(LoggerConfig.getLogParserSemaphoreMonitorPollUnit(), "milliseconds");
    }

    @Test
    public void testGetLogWritersPool() {
        Assert.assertEquals(LoggerConfig.getLogWritersPool(), 10);
    }

    @Test
    public void testGetParserConfigDirectory() {
        Assert.assertEquals(LoggerConfig.getParserConfigDirectory(), "/etc/opt/niara/analyzer/logger");
    }

    @Test
    public void testGetLogParsersRateLimitRate() {
        Assert.assertEquals(LoggerConfig.getLogParsersRateLimitRate(), 2);
    }

    @Test
    public void testGetLogParsersRateLimitInterval() {
        Assert.assertEquals(LoggerConfig.getLogParsersRateLimitInterval(), 1L);
    }

    @Test
    public void testStatsDHost() {
        Assert.assertEquals(LoggerConfig.getStatsDHost(), "localhost");
    }

    @Test
    public void testStatsDPort() {
        Assert.assertEquals(LoggerConfig.getStatsDPort(), 8125);
    }

}
