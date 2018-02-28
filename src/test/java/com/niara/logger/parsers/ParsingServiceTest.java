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

import com.niara.logger.exceptions.GrokException;
import com.niara.logger.exceptions.InvalidTimestampException;
import com.niara.logger.exceptions.MatchDropConditionException;
import com.niara.logger.exceptions.UnsupportedConditionException;
import com.niara.logger.utils.GrokHandler;
import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Output;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ParsingServiceTest {

    private JSONObject constructConditionalParsingConfig(String parsingConfig) {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = null;
        try {
            jsonObject = (JSONObject) jsonParser.parse(parsingConfig);
        } catch (ParseException e) {
        }

        return jsonObject;
    }

    @BeforeClass
    public void setUp() {
        LoggerConfig.init("TEST");
        GrokHandler.init();

        String conditionalParsingConfig = "{\"name\":\"TestConditional\",\"input\":\"log_conditional\",\"output\":\"test\",\"groks\":[{\"pattern\":\"%{DATA:rtype}:\\s+%{GREEDYDATA:message}\"}],\"parsers\":[{\"type\":\"KeyValueParser\",\"params\":{\"source\":\"message\",\"field_separator\":\",\",\"value_separator\":\"=\"}}],\"conditional_mappings\":{\"conditions\":[\"rtype\"]},\"drop\":{\"match\":[{\"source\":\"A\",\"in\":{\"values\":[\"B\"]}}]},\"rtype\":{\"START\":{}}}";
        LoggerConfig.getLogInput2Config().put("log_conditional", constructConditionalParsingConfig(conditionalParsingConfig));

        String unConditionalParsingConfig = "{\"name\":\"TestUnConditional\",\"input\":\"log_unconditional\",\"output\":\"test\",\"groks\":[{\"pattern\":\"%{DATA:rtype}:\\s+%{GREEDYDATA:message}\"}],\"parsers\":[{\"type\":\"KeyValueParser\",\"params\":{\"source\":\"message\",\"field_separator\":\",\",\"value_separator\":\"=\"}}],\"drop\":{\"match\":[{\"source\":\"A\",\"in\":{\"values\":[\"B\"]}}]},\"mappings\":[]}";
        LoggerConfig.getLogInput2Config().put("log_unconditional", constructConditionalParsingConfig(unConditionalParsingConfig));

        String badConfig = "{\"name\":\"TestConditional\",\"input\":\"log_conditional\",\"output\":\"test\",\"groks\":[{\"pattern\":\"%{DATA:rtype}:\\s+%{GREEDYDATA:message}\"}],\"parsers\":[{\"type\":\"KeyValueParser\",\"params\":{\"source\":\"record\",\"field_separator\":\",\",\"value_separator\":\"=\"}}],\"conditional_mappings\":{\"conditions\":[\"rtype\"]},\"rtype\":{\"START\":{}}}";
        LoggerConfig.getLogInput2Config().put("bad_config", constructConditionalParsingConfig(badConfig));

        String goodConfig = "{\"name\":\"TestGoodConfig\",\"input\":\"log_good\",\"output\":\"test\",\"groks\":[{\"pattern\":\"%{DATA:rtype}:\\s+%{GREEDYDATA:message}\"}],\"parsers\":[{\"type\":\"KeyValueParser\",\"params\":{\"source\":\"message\",\"field_separator\":\",\",\"value_separator\":\"=\"}}],\"conditional_mappings\":{\"conditions\":[\"rtype\"]},\"rtype\":{\"START\":{\"mappings\": [{\"source\":\"A\",\"target\":\"A\",\"transformers\":[{\"transformer\":\"to_long\"}]}]}},\"discard_fields\":[]}";
        LoggerConfig.getLogInput2Config().put("good_config", constructConditionalParsingConfig(goodConfig));

        String dropAfterGrokParsingConfig = "{\"name\":\"DropAfterGrok\",\"input\":\"log_drop_after_grok\",\"output\":\"test\",\"groks\":[{\"pattern\":\"%{DATA:rtype}:\\s+%{GREEDYDATA:message}\"}],\"parsers\":[{\"type\":\"KeyValueParser\",\"params\":{\"source\":\"message\",\"field_separator\":\",\",\"value_separator\":\"=\"}}],\"drop\":{\"match\":[{\"source\":\"rtype\",\"in\":{\"values\":[\"START\"]}}]},\"mappings\":[]}";
        LoggerConfig.getLogInput2Config().put("log_drop_after_grok", constructConditionalParsingConfig(dropAfterGrokParsingConfig));

        String regexParsingConfig_in = "{\"name\":\"TestUnConditional\",\"input\":\"log_unconditional\",\"output\":\"test\",\"groks\":[{\"pattern\":\"%{DATA:rtype}:\\s+%{GREEDYDATA:message}\"}],\"parsers\":[{\"type\":\"KeyValueParser\",\"params\":{\"source\":\"message\",\"field_separator\":\",\",\"value_separator\":\"=\"}}],\"drop\":{\"match\":[{\"source\":\"A\",\"in\":{\"regex\":true,\"values\":[\"[A-Z]\"]}}]},\"mappings\":[]}";
        LoggerConfig.getLogInput2Config().put("log_regex_in", constructConditionalParsingConfig(regexParsingConfig_in));

        String regexParsingConfig_not_in = "{\"name\":\"TestUnConditional\",\"input\":\"log_unconditional\",\"output\":\"test\",\"groks\":[{\"pattern\":\"%{DATA:rtype}:\\s+%{GREEDYDATA:message}\"}],\"parsers\":[{\"type\":\"KeyValueParser\",\"params\":{\"source\":\"message\",\"field_separator\":\",\",\"value_separator\":\"=\"}}],\"drop\":{\"match\":[{\"source\":\"A\",\"not_in\":{\"regex\":true,\"values\":[\"[A-Z]\"]}}]},\"mappings\":[]}";
        LoggerConfig.getLogInput2Config().put("log_regex_not_in", constructConditionalParsingConfig(regexParsingConfig_not_in));

        String invalidTimestampConfig = "{\"name\":\"TestInvalidTimestampConfig\",\"input\":\"log_good\",\"output\":\"test\",\"groks\":[{\"pattern\":\"%{DATA:rtype}:\\s+%{GREEDYDATA:message}\"}],\"parsers\":[{\"type\":\"KeyValueParser\",\"params\":{\"source\":\"message\",\"field_separator\":\",\",\"value_separator\":\"=\"}}],\"conditional_mappings\":{\"conditions\":[\"rtype\"]},\"rtype\":{\"START\":{\"mappings\": []}}}";
        LoggerConfig.getLogInput2Config().put("invalid_timestamp_config", constructConditionalParsingConfig(invalidTimestampConfig));
    }

    @AfterClass
    public void tearDown() {
        LoggerConfig.getLogInput2Config().remove("log_conditional");

        LoggerConfig.getLogInput2Config().remove("log_unconditional");

        LoggerConfig.getLogInput2Config().remove("bad_config");

        LoggerConfig.getLogInput2Config().remove("good_config");

        LoggerConfig.getLogInput2Config().remove("log_regex_in");

        LoggerConfig.getLogInput2Config().remove("log_regex_not_in");

        LoggerConfig.getLogInput2Config().remove("invalid_timestamp_config");
    }

    @Test(expectedExceptions = {UnsupportedConditionException.class},
          expectedExceptionsMessageRegExp = "Unsupported condition: UNKNOWN seen for inputLogType: log_conditional")
    public void testUnsupportedConditionException() throws Exception {
        String logLine = "UNKNOWN: A=B, C=D, E=F";
        ParsingService parsingService = new ParsingService("log_conditional");
        parsingService.parse(logLine);
    }

    @Test(expectedExceptions = {MatchDropConditionException.class},
        expectedExceptionsMessageRegExp = "Matched global drop condition for inputLogType: log_conditional")
    public void testMatchDropConditionExceptionWithConditional() throws Exception {
        String logLine = "START: A=B, C=D, E=F";
        ParsingService parsingService = new ParsingService("log_conditional");
        parsingService.parse(logLine);
    }

    @Test(expectedExceptions = {MatchDropConditionException.class},
        expectedExceptionsMessageRegExp = "Matched global drop condition for inputLogType: log_unconditional")
    public void testMatchDropConditionExceptionWithoutConditional() throws Exception {
        String logLine = "START: A=B, C=D, E=F";
        ParsingService parsingService = new ParsingService("log_unconditional");
        parsingService.parse(logLine);
    }

    @Test(expectedExceptions = {MatchDropConditionException.class},
        expectedExceptionsMessageRegExp = "Matched global drop condition after Grokking for inputLogType: log_drop_after_grok")
    public void testMatchDropConditionExceptionAfterGrok() throws Exception {
        String logLine = "START: A=B, C=D, E=F";
        ParsingService parsingService = new ParsingService("log_drop_after_grok");
        parsingService.parse(logLine);
    }

    @Test(expectedExceptions = {GrokException.class},
        expectedExceptionsMessageRegExp = "Grok matching failed with grok pattern.*")
    public void testGrokException() throws Exception {
        String logLine = "12456 A=B, C=D, E=F";
        ParsingService parsingService = new ParsingService("log_conditional");
        parsingService.parse(logLine);
    }

    @Test(expectedExceptions = {Exception.class},
            expectedExceptionsMessageRegExp = "source 'record' does not exist for log_type: 'bad_config'.")
    public void testBadConfig() throws Exception {
        String logLine = "START: A=B, C=D, E=F";
        ParsingService parsingService = new ParsingService("bad_config");
        parsingService.parse(logLine);
    }

    @Test(expectedExceptions = {MatchDropConditionException.class},
            expectedExceptionsMessageRegExp = "Matched global drop condition for inputLogType: log_regex_in")
    public void testRegexInListDrop() throws Exception {
        String logLine = "START: A=B, C=D, E=F";
        ParsingService parsingService = new ParsingService("log_regex_in");
        parsingService.parse(logLine);
    }

    @Test(expectedExceptions = {MatchDropConditionException.class},
            expectedExceptionsMessageRegExp = "Matched global drop condition for inputLogType: log_regex_not_in")
    public void testRegexNotInListDrop() throws Exception {
        String logLine = "START: A=12345, C=D, E=F";
        ParsingService parsingService = new ParsingService("log_regex_not_in");
        parsingService.parse(logLine);
    }

    @Test(expectedExceptions = {InvalidTimestampException.class}, expectedExceptionsMessageRegExp = "Invalid Timestamp value: 0")
    public void testInvalidTimestampConfig() throws Exception {
        String logLine = "START: A=1, C=2, D=3";
        ParsingService parsingService = new ParsingService("invalid_timestamp_config");
        parsingService.parse(logLine);
    }

    @Test
    public void testGoodConfig() throws Exception {
        String logLine = "START: A=1, B=2, timestamp=3";
        ParsingService parsingService = new ParsingService("good_config");
        Output parsedOutput = parsingService.parse(logLine);
        Assert.assertEquals(parsedOutput.timestamp(), 3L);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse((String) parsedOutput.value());
        Assert.assertEquals(jsonObject.size(), 5);
        Assert.assertEquals(jsonObject.get("A"), 1L);
        Assert.assertEquals(jsonObject.get("B"), "2");
        Assert.assertEquals(jsonObject.get("timestamp"), "3");
        Assert.assertEquals(jsonObject.get("rtype"), "START");
        Assert.assertEquals(jsonObject.get("message"), "A=1, B=2, timestamp=3");
    }

}