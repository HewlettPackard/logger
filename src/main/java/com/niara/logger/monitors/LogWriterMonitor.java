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

import com.niara.logger.stats.LogCounter;
import com.niara.logger.writers.LogWriter;
import com.niara.logger.stats.NonBlockingStatsDClient;
import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Tuple;
import com.niara.logger.writers.LogWriterCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class LogWriterMonitor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LogWriterMonitor.class);

    private final CountDownLatch logWriterLatch;
    private final ExecutorService logWriterService;
    private final ExecutorCompletionService<LogWriterCounter> logWriterECS;
    private final NonBlockingStatsDClient stats;

    public LogWriterMonitor(final CountDownLatch logWriterLatch,
                            final ExecutorService logWriterService,
                            final ExecutorCompletionService<LogWriterCounter> logWriterECS) {
        this.logWriterLatch = logWriterLatch;
        this.logWriterService = logWriterService;
        this.logWriterECS = logWriterECS;
        this.stats = NonBlockingStatsDClient.getTSDBStats();
    }

    public void run() {
        long count = 0;
        long previous = System.currentTimeMillis();
        boolean terminated;
        LogCounter logCounter = new LogCounter(stats);

        try {
            do {
                terminated = logWriterService.awaitTermination(LoggerConfig.getLogWriterMonitorPollInterval(), TimeUnit.valueOf(LoggerConfig.getLogWriterMonitorPollUnit().toUpperCase()));
                Future<LogWriterCounter> future;
                // Drain the queue as much as possible.
                while ((future = logWriterECS.poll()) != null) {
                    LogWriterCounter logWriterCounter = future.get();
                    count += logWriterCounter.getSuccessCount();
                    logCounter.updateWriterStats(logWriterCounter);
                    long now = System.currentTimeMillis();
                    if (stats != null) {
                        if (now - previous >= LoggerConfig.getStatsInterval()) {
                            logCounter.savePeriodicWriterStats();
                            logger.info("LogWriter wrote {} logs from {} to {}", count, previous, now);
                            count = 0;
                            previous = now;
                        }
                    }
                }
            } while (!terminated);

            if (!logWriterService.isTerminated())
                logger.error("LogWriter is not terminated after shutdown.");
        } catch (Exception e) {
            logger.error("LogWriterServiceException", e);
        }

        logger.info("LogWriter service terminated, signalling that all writers have finished.");

        if (stats != null)
            stats.stop();

        // Now signal that all writers have shutdown. This is used during shutdown process.
        logWriterLatch.countDown();
    }

}