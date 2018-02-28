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

package com.niara.logger.stats;

import com.niara.logger.utils.LoggerConfig;
import com.timgroup.statsd.StatsDClient;



public class NonBlockingStatsDClient {
    private static final String STAT_PREFIX = "logger";
    private StatsDClient statsDClient;

    private NonBlockingStatsDClient(StatsDClient statsDClient) {
        this.statsDClient = statsDClient;
    }

    public static NonBlockingStatsDClient getTSDBStats() {
        NonBlockingStatsDClient nonBlockingStatsDClient;
        StatsDClient statsDClient;
        try {
            statsDClient = new com.timgroup.statsd.NonBlockingStatsDClient(STAT_PREFIX, LoggerConfig.getStatsDHost(), LoggerConfig.getStatsDPort());
        } catch (Exception e) {
            statsDClient = null;
        }

        nonBlockingStatsDClient = new NonBlockingStatsDClient(statsDClient);
        return nonBlockingStatsDClient;
    }

    public void saveGaugeStat(String stat, long value) {
        statsDClient.recordGaugeValue(stat, value);
    }

    public void saveGuageStat(String stat, double value) {
        statsDClient.recordGaugeValue(stat, value);
    }

    public void saveTimerStat(String stat, long value) {
        statsDClient.recordExecutionTime(stat, value);
    }

    public void stop() {
        statsDClient.stop();
    }
}
