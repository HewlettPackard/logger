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

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by bvarghese on 3/24/17.
 */
public class LogParserCounterTest {

    private static String inputLogType = "log_win_ad_security";
    private static String outputLogType = "ad_log";

    @Test
    public void testGetSuccessAndErrorRateWithoutMatchDropAndUnsupportedConditionErrors() {
        LogParserCounter parserCounter = new LogParserCounter(inputLogType, outputLogType, 100);
        for (int i = 0; i < 5; i++) {
            parserCounter.incrementUnexpectedErrorCount();
            parserCounter.incrementValidationErrorCount();
            parserCounter.incrementInvalidTimestampErrorCount();
        }

        Assert.assertEquals(parserCounter.getInputLogType(), inputLogType);
        Assert.assertEquals(parserCounter.getOutputLogType(), outputLogType);
        Assert.assertEquals(parserCounter.getSuccessRate(), 85.0);
        Assert.assertEquals(parserCounter.getErrorRate(), 15.0);
        Assert.assertEquals(parserCounter.getSuccessfulParsedLogCount(), 85);
    }

    @Test
    public void testGetSuccessAndErrorRateWithMatchDropAndUnsupportedConditionErrors() {
        LogParserCounter parserCounter = new LogParserCounter(inputLogType, outputLogType, 100);
        for (int i = 0; i < 5; i++) {
            parserCounter.incrementUnexpectedErrorCount();
            parserCounter.incrementValidationErrorCount();
            parserCounter.incrementInvalidTimestampErrorCount();
            parserCounter.incrementGrokErrorCount();
        }

        for (int i = 0; i < 10; i++) {
            parserCounter.incrementMatchDropConditionCount();
            parserCounter.incrementUnsupportedConditionCount();
        }

        Assert.assertEquals(parserCounter.getInputLogType(), inputLogType);
        Assert.assertEquals(parserCounter.getOutputLogType(), outputLogType);
        Assert.assertEquals(parserCounter.getSuccessRate(), 80.0);
        Assert.assertEquals(parserCounter.getErrorRate(), 20.0);
    }

    @Test
    public void testGetSuccessAndErrorRateWithAllFailures() {
        LogParserCounter parserCounter = new LogParserCounter(inputLogType, outputLogType, 100);
        for (int i = 0; i < 100; i++) {
            parserCounter.incrementUnexpectedErrorCount();
        }

        Assert.assertEquals(parserCounter.getInputLogType(), inputLogType);
        Assert.assertEquals(parserCounter.getOutputLogType(), outputLogType);
        Assert.assertEquals(parserCounter.getSuccessRate(), 0.0);
        Assert.assertEquals(parserCounter.getErrorRate(), 100.0);
    }

    @Test
    public void testGetSuccessAndErrorRateWithAllSuccess() {
        LogParserCounter parserCounter = new LogParserCounter(inputLogType, outputLogType, 100);

        Assert.assertEquals(parserCounter.getInputLogType(), inputLogType);
        Assert.assertEquals(parserCounter.getOutputLogType(), outputLogType);
        Assert.assertEquals(parserCounter.getSuccessRate(), 100.0);
        Assert.assertEquals(parserCounter.getErrorRate(), 0.0);
    }

    @Test
    public void testGetSuccessAndErrorRateWithOnlyUnsupportedConditions() {
        LogParserCounter parserCounter = new LogParserCounter(inputLogType, outputLogType, 100);
        for (int i = 0; i < 50; i++) {
            parserCounter.incrementUnsupportedConditionCount();
            parserCounter.incrementMatchDropConditionCount();
        }

        Assert.assertEquals(parserCounter.getInputLogType(), inputLogType);
        Assert.assertEquals(parserCounter.getOutputLogType(), outputLogType);
        Assert.assertEquals(parserCounter.getSuccessRate(), 100.0);
        Assert.assertEquals(parserCounter.getErrorRate(), 0.0);
        Assert.assertEquals(parserCounter.getUnsupportedConditionCount(), 50);
        Assert.assertEquals(parserCounter.getMatchDropConditionCount(), 50);
    }

}
