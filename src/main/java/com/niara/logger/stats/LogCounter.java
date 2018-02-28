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

import com.niara.logger.parsers.LogParserCounter;
import com.niara.logger.writers.LogWriterCounter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bvarghese on 4/3/17.
 */
public class LogCounter {

    private long total = 0;
    private Map<String, Long> inputTypeCounter = new HashMap<>();
    private Map<String, Long> outputTypeCounter = new HashMap<>();
    private Map<String, Long> inputTypeTotalCounter = new HashMap<>();
    private Map<String, Long> inputTypeErrorCounter = new HashMap<>();
    private final NonBlockingStatsDClient nonBlockingStatsDClient;

    public LogCounter(NonBlockingStatsDClient nonBlockingStatsDClient) {
       this.nonBlockingStatsDClient = nonBlockingStatsDClient;
    }

    public long getTotal() {
        return total;
    }

    public void updateParserStats(LogParserCounter logParserCounter) {
        total += logParserCounter.getSuccessfulParsedLogCount();

        String inputLogType = logParserCounter.getInputLogType();
        if (inputTypeCounter.containsKey(inputLogType)) {
            inputTypeCounter.put(inputLogType, inputTypeCounter.get(inputLogType) + logParserCounter.getSuccessfulParsedLogCount());
            inputTypeErrorCounter.put(inputLogType, inputTypeErrorCounter.get(inputLogType) + logParserCounter.getErrorCount());
            inputTypeTotalCounter.put(inputLogType, inputTypeTotalCounter.get(inputLogType) + logParserCounter.getTotalCount());
        } else {
            inputTypeCounter.put(inputLogType, logParserCounter.getSuccessfulParsedLogCount());
            inputTypeErrorCounter.put(inputLogType, logParserCounter.getErrorCount());
            inputTypeTotalCounter.put(inputLogType, logParserCounter.getTotalCount());
        }

        String outputLogType = logParserCounter.getOutputLogType();
        if (outputTypeCounter.containsKey(outputLogType)) {
            outputTypeCounter.put(outputLogType, outputTypeCounter.get(outputLogType) + logParserCounter.getSuccessfulParsedLogCount());
        } else {
            outputTypeCounter.put(outputLogType, logParserCounter.getSuccessfulParsedLogCount());
        }
    }

    public void updateReaderStats(long count, String inputLogType) {
        total += count;
        if (inputTypeCounter.containsKey(inputLogType)) {
            inputTypeCounter.put(inputLogType, inputTypeCounter.get(inputLogType) + count);
        } else {
            inputTypeCounter.put(inputLogType, count);
        }

    }

    public void updateWriterStats(LogWriterCounter logWriterCounter) {
        total += logWriterCounter.getTotalCount();

        String inputLogType = logWriterCounter.getInputLogType();
        if (inputTypeCounter.containsKey(inputLogType)) {
            inputTypeCounter.put(inputLogType, inputTypeCounter.get(inputLogType) + logWriterCounter.getSuccessCount());
            inputTypeErrorCounter.put(inputLogType, inputTypeErrorCounter.get(inputLogType) + logWriterCounter.getErrorCount());
            inputTypeTotalCounter.put(inputLogType, inputTypeTotalCounter.get(inputLogType) + logWriterCounter.getTotalCount());
        } else {
            inputTypeCounter.put(inputLogType, logWriterCounter.getSuccessCount());
            inputTypeErrorCounter.put(inputLogType, logWriterCounter.getErrorCount());
            inputTypeTotalCounter.put(inputLogType, logWriterCounter.getTotalCount());
        }

        String outputLogType = logWriterCounter.getOutputLogType();
        if (outputTypeCounter.containsKey(outputLogType)) {
            outputTypeCounter.put(outputLogType, outputTypeCounter.get(outputLogType) + logWriterCounter.getSuccessCount());
        } else {
            outputTypeCounter.put(outputLogType, logWriterCounter.getSuccessCount());
        }
    }

    public void saveInstantParserStats(LogParserCounter logParserCounter) {
        nonBlockingStatsDClient.saveGaugeStat("parser.latest-" + logParserCounter.getInputLogType() + "-time", System.currentTimeMillis());
        nonBlockingStatsDClient.saveGaugeStat("parser." + logParserCounter.getInputLogType() + ".error-count", logParserCounter.getErrorCount());
        nonBlockingStatsDClient.saveGaugeStat("parser." + logParserCounter.getInputLogType() + ".unexpected-error-count", logParserCounter.getUnexpectedErrorCount());
        nonBlockingStatsDClient.saveGaugeStat("parser." + logParserCounter.getInputLogType() + ".validation-error-count", logParserCounter.getValidationErrorCount());
        nonBlockingStatsDClient.saveGaugeStat("parser." + logParserCounter.getInputLogType() + ".timestamp-error-count", logParserCounter.getInvalidTimestampErrorCount());
        nonBlockingStatsDClient.saveGaugeStat("parser." + logParserCounter.getInputLogType() + ".grok-error-count", logParserCounter.getGrokErrorCount());
        nonBlockingStatsDClient.saveGaugeStat("parser." + logParserCounter.getInputLogType() + ".match-drop-condition-count", logParserCounter.getMatchDropConditionCount());
        nonBlockingStatsDClient.saveGaugeStat("parser." + logParserCounter.getInputLogType() + ".unsupported-condition-count", logParserCounter.getUnsupportedConditionCount());
    }

    private void clear() {
        total = 0;
        inputTypeCounter.clear();
        inputTypeErrorCounter.clear();
        inputTypeTotalCounter.clear();
    }

    public void savePeriodicParserStats(LogParserCounter logParserCounter) {
        nonBlockingStatsDClient.saveGaugeStat("parser.total-count", total);

        for (Map.Entry<String, Long> entry: inputTypeCounter.entrySet()) {
            nonBlockingStatsDClient.saveGaugeStat("parser." + entry.getKey() + ".count", entry.getValue());
            nonBlockingStatsDClient.saveGaugeStat("parser." + entry.getKey() + ".total-count", inputTypeTotalCounter.get(entry.getKey()));
            nonBlockingStatsDClient.saveGuageStat("parser." + entry.getKey() + ".success-rate", logParserCounter.calculatePercentage(entry.getValue(), inputTypeTotalCounter.get(entry.getKey())));
            nonBlockingStatsDClient.saveGuageStat("parser." + entry.getKey() + ".error-rate", logParserCounter.calculatePercentage(inputTypeErrorCounter.get(entry.getKey()), inputTypeTotalCounter.get(entry.getKey())));
        }

        for (Map.Entry<String, Long> entry: outputTypeCounter.entrySet())
            nonBlockingStatsDClient.saveGaugeStat("parser." + entry.getKey() + ".count", entry.getValue());

        clear();
    }

    public void savePeriodicReaderStats() {
        nonBlockingStatsDClient.saveGaugeStat("reader.last-operation-time", System.currentTimeMillis() * 1000);
        nonBlockingStatsDClient.saveGaugeStat("reader.total-count", total);

        for (Map.Entry<String, Long> entry: inputTypeCounter.entrySet()) {
            nonBlockingStatsDClient.saveGaugeStat("reader." + entry.getKey() + ".count", entry.getValue());
        }

        clear();
    }

    public void savePeriodicWriterStats() {
        nonBlockingStatsDClient.saveGaugeStat("writer.total-count", total);
        nonBlockingStatsDClient.saveGaugeStat("writer.last-operation-time", System.currentTimeMillis() * 1000);

        for (Map.Entry<String, Long> entry: inputTypeCounter.entrySet()) {
            nonBlockingStatsDClient.saveGaugeStat("writer." + entry.getKey() + ".total-count", inputTypeTotalCounter.get(entry.getKey()));
            nonBlockingStatsDClient.saveGuageStat("writer." + entry.getKey() + ".success-count", inputTypeCounter.get(entry.getKey()));
            nonBlockingStatsDClient.saveGuageStat("writer." + entry.getKey() + ".error-count", inputTypeErrorCounter.get(entry.getKey()));
        }

        for (Map.Entry<String, Long> entry: outputTypeCounter.entrySet())
            nonBlockingStatsDClient.saveGaugeStat("writer." + entry.getKey() + ".count", entry.getValue());

        clear();
    }

}