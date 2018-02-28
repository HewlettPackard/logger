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
import com.niara.logger.stats.LogCounter;
import com.niara.logger.stats.NonBlockingStatsDClient;
import com.niara.logger.utils.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class LogParserMonitor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LogParserMonitor.class);
    private final Semaphore logParserSemaphore;
    private final CountDownLatch logParserLatch;
    private final ExecutorService logParserService;
    private final ExecutorCompletionService<LogParserCounter> logParserECS;
    private final NonBlockingStatsDClient stats;

    public LogParserMonitor(final Semaphore logParserSemaphore,
                            final CountDownLatch logParserLatch,
                            final ExecutorService logParserService,
                            final ExecutorCompletionService<LogParserCounter> logParserECS) {
        this.logParserSemaphore = logParserSemaphore;
        this.logParserLatch = logParserLatch;
        this.logParserService = logParserService;
        this.logParserECS = logParserECS;
        this.stats = NonBlockingStatsDClient.getTSDBStats();
    }

    public void run() {
        long previous = System.currentTimeMillis();
        boolean terminated = false;
        LogCounter logCounter = new LogCounter(stats);

        do {
            try {
                terminated = logParserService.awaitTermination(
                    LoggerConfig.getLogParserMonitorPollInterval(),
                    TimeUnit.valueOf(LoggerConfig.getLogParserMonitorPollUnit().toUpperCase()));
            } catch (InterruptedException e) {
                logger.error("LogParserMonitor InterruptedException", e);
            }
            Future<LogParserCounter> future;
            // Drain the queue as much as possible.
            while ((future = logParserECS.poll()) != null) {
                try {
                    LogParserCounter parserCounter = future.get();
                    logCounter.updateParserStats(parserCounter);
                    long now = System.currentTimeMillis();
                    logger.info("Parser parsed {} logs from {} to {}", logCounter.getTotal(), previous, now);
                    if (stats != null) {
                        logCounter.saveInstantParserStats(parserCounter);
                        if (now - previous >= LoggerConfig.getStatsInterval()) {
                            logger.debug("Parser parsed {} logs from {} to {}", logCounter.getTotal(), previous, now);
                            logCounter.savePeriodicParserStats(parserCounter);
                            previous = now;
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error("LogParserMonitor InterruptedException", e);
                } catch (ExecutionException e) {
                    logger.error("LogParser failed to parse", e);
                } catch (Exception e) {
                    // Any runtime exception.
                    logger.error("Unexpected exception in parser service", e);
                } finally {
                    logParserSemaphore.release();
                }
            }
        } while (!terminated);

        if (!logParserService.isTerminated())
            logger.error("LogParser is not terminated after shutdown.");

        logger.info("LogParser service terminated, signalling that all parsers have finished, and their output have been submitted to log writers.");

        if (stats != null)
            stats.stop();

        // All the parses have submitted parsed output to respective log writes.
        // Now signal to shutdown logReaderToLogParser.
        logParserLatch.countDown();
    }

}
