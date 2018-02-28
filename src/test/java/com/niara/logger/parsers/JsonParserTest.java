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

import com.niara.logger.utils.LoggerConfig;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class JsonParserTest {

    private Matcher topMatcher;

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
        Parser.init(LoggerConfig.getTimestampKeys(), LoggerConfig.getTimestampDefault());
        String patternString = ".*";
        String text = "foo";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(text);
        this.topMatcher = matcher;
    }

    @Test
    public void testJsonParser() {
        String jsonLog = "{\"a\":\"A\", \"b\":\"B\", \"c\":2, \"d\":\"D\", \"e\": \"E\"}";
        Parser jsonParser = new JsonParser(jsonLog);
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("jsonLog", jsonLog);
        jsonParser.setEvent(eventMap);
        jsonParser.setTopMatcher(this.topMatcher);
        JSONObject parserConfig = new JSONObject();
        JSONObject params = new JSONObject();
        params.put("source", "jsonLog");
        parserConfig.put("params", params);
        jsonParser.setInputBuilder(parserConfig);
        jsonParser.parse();
        Map<String, Object> parsedMap = jsonParser.getEventMap();
        Assert.assertEquals(parsedMap.size(), 6);
        Assert.assertEquals(parsedMap.get("a"), "A");
        Assert.assertEquals(parsedMap.get("b"), "B");
        Assert.assertEquals(parsedMap.get("c"), 2L);
        Assert.assertEquals(parsedMap.get("d"), "D");
        Assert.assertEquals(parsedMap.get("e"), "E");
        Assert.assertEquals(parsedMap.get("jsonLog"), jsonLog);
    }

    @Test
    public void testJsonParserWithInvalidJSONLogLine() {
        String jsonLog = "{a:\"A\", \"b\":\"B\", \"c\":2, \"d\":\"D\", \"e\": \"E\"}";
        Parser jsonParser = new JsonParser(jsonLog);
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("jsonLog", jsonLog);
        jsonParser.setEvent(eventMap);
        jsonParser.setTopMatcher(this.topMatcher);
        JSONObject parserConfig = new JSONObject();
        JSONObject params = new JSONObject();
        params.put("source", "jsonLog");
        parserConfig.put("params", params);
        jsonParser.setInputBuilder(parserConfig);
        jsonParser.parse();
        Map<String, Object> parsedMap = jsonParser.getEventMap();
        Assert.assertEquals(parsedMap.size(), 1);
        Assert.assertEquals(parsedMap.get("jsonLog"), jsonLog);
    }
}
