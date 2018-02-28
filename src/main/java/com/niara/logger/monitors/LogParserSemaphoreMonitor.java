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

import com.niara.logger.stats.NonBlockingStatsDClient;
import com.niara.logger.utils.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class LogParserSemaphoreMonitor implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(LogParserSemaphoreMonitor.class);

    private final Semaphore parserSemaphore;
    private final CountDownLatch logParserLatch;
    private final NonBlockingStatsDClient nonBlockingStatsDClient;

    public LogParserSemaphoreMonitor(final Semaphore parserSemaphore, final CountDownLatch logParserLatch) {
        this.parserSemaphore = parserSemaphore;
        this.logParserLatch = logParserLatch;
        this.nonBlockingStatsDClient = NonBlockingStatsDClient.getTSDBStats();
    }

    public void run() {
        while (logParserLatch.getCount() != 0) {
            logger.info("Parser parserSemaphore queue length is {}, and available permits are {}", parserSemaphore.getQueueLength(), parserSemaphore.availablePermits());
            nonBlockingStatsDClient.saveGaugeStat("parser.last-operation-time", System.currentTimeMillis() * 1000);
            nonBlockingStatsDClient.saveGaugeStat("parser.parserSemaphore.queue-length", parserSemaphore.getQueueLength());
            nonBlockingStatsDClient.saveGaugeStat("parser.parserSemaphore.available-permits", parserSemaphore.availablePermits());

            TimeUnit durationUnit = TimeUnit.valueOf(LoggerConfig.getLogParserSemaphoreMonitorPollUnit().toUpperCase());
            long duration = durationUnit.toMillis(LoggerConfig.getLogParserSemaphoreMonitorPollInterval());
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
