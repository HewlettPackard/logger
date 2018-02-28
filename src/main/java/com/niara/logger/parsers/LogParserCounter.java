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

package com.niara.logger.parsers;

/**
 * Created by bvarghese on 3/24/17.
 */
public class LogParserCounter {

    private String inputLogType;
    private String outputLogType;
    private long totalCount;
    private long grokErrorCount;
    private long unexpectedErrorCount;
    private long validationErrorCount;
    private long invalidTimestampErrorCount;
    private long matchDropConditionCount;
    private long unsupportedConditionCount;

    public LogParserCounter(String inputLogType, String outputLogType, long total) {
        this.inputLogType = inputLogType;
        this.outputLogType = outputLogType;
        totalCount = total;
        grokErrorCount = unexpectedErrorCount = validationErrorCount = invalidTimestampErrorCount = matchDropConditionCount = unsupportedConditionCount = 0;
    }

    protected void incrementGrokErrorCount() {
        grokErrorCount += 1;
    }

    protected void incrementUnexpectedErrorCount() {
        unexpectedErrorCount += 1;
    }

    protected void incrementValidationErrorCount() {
        validationErrorCount += 1;
    }

    protected void incrementInvalidTimestampErrorCount() {
       invalidTimestampErrorCount += 1;
    }

    protected void incrementMatchDropConditionCount() {
        matchDropConditionCount += 1;
    }

    protected void incrementUnsupportedConditionCount() {
        unsupportedConditionCount += 1;
    }

    public String getInputLogType() {
        return inputLogType;
    }

    public String getOutputLogType() {
        return outputLogType;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public long getGrokErrorCount() {
        return grokErrorCount;
    }

    public long getUnexpectedErrorCount() {
        return unexpectedErrorCount;
    }

    public long getValidationErrorCount() {
        return validationErrorCount;
    }

    public long getInvalidTimestampErrorCount() {
        return invalidTimestampErrorCount;
    }

    public long getMatchDropConditionCount() {
        return matchDropConditionCount;
    }

    public long getUnsupportedConditionCount() {
        return unsupportedConditionCount;
    }

    public long getExpectedErrorCount() {
        return getMatchDropConditionCount() + getUnsupportedConditionCount();
    }

    public long getErrorCount() {
        return getUnexpectedErrorCount() + getGrokErrorCount() + getValidationErrorCount() + getInvalidTimestampErrorCount();
    }

    public long getSuccessfulParsedLogCount() {
        return getTotalCount() - getErrorCount();
    }

    public double getSuccessRate() {
        return calculatePercentage(getSuccessfulParsedLogCount(), getTotalCount());
    }

    public double getErrorRate() {
        return (getErrorCount()  / (getTotalCount() + 0.0)) * 100.0;
    }

    public double calculatePercentage(long number, long total) {
        return (number / (total + 0.0)) * 100.0;
    }

}
