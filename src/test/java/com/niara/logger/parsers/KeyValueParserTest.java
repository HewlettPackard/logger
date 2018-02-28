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


public class KeyValueParserTest {

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
    public void testKeyValueParser() {
        String keyValueLog = "a=A,b=B,c=\"C=C=C\",d=D,e=E";
        Parser keyValueParser = new KeyValueParser(keyValueLog);
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("keyValueLog", keyValueLog);
        keyValueParser.setEvent(eventMap);
        keyValueParser.setTopMatcher(this.topMatcher);
        JSONObject parserConfig = new JSONObject();
        JSONObject params = new JSONObject();
        params.put("source", "keyValueLog");
        params.put("field_separator", ",");
        params.put("value_separator", "=");
        parserConfig.put("params", params);
        keyValueParser.setInputBuilder(parserConfig);
        keyValueParser.parse();
        Map<String, Object> parsedMap = keyValueParser.getEventMap();
        Assert.assertEquals(parsedMap.size(), 6);
        Assert.assertEquals(parsedMap.get("a"), "A");
        Assert.assertEquals(parsedMap.get("b"), "B");
        Assert.assertEquals(parsedMap.get("c"), "C=C=C");
        Assert.assertEquals(parsedMap.get("d"), "D");
        Assert.assertEquals(parsedMap.get("e"), "E");
        Assert.assertEquals(parsedMap.get("keyValueLog"), keyValueLog);
    }

    @Test
    public void testKeyValueParserWithTargetField() {
        String keyValueLog = "a=A,b=B,c=\"C=C=C\",d=D,e=E";
        Parser keyValueParser = new KeyValueParser(keyValueLog);
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("keyValueLog", keyValueLog);
        keyValueParser.setEvent(eventMap);
        keyValueParser.setTopMatcher(this.topMatcher);
        JSONObject parserConfig = new JSONObject();
        JSONObject params = new JSONObject();
        params.put("source", "keyValueLog");
        params.put("field_separator", ",");
        params.put("value_separator", "=");
        params.put("target", "target_field");
        parserConfig.put("params", params);
        keyValueParser.setInputBuilder(parserConfig);
        keyValueParser.parse();
        Map<String, Object> parsedMap = keyValueParser.getEventMap();
        Assert.assertEquals(parsedMap.size(), 2);
        Assert.assertEquals(parsedMap.get("keyValueLog"), keyValueLog);
        Map<String, Object> parsedKeyValueMap = (Map<String, Object>) parsedMap.get("target_field");
        Assert.assertEquals(parsedKeyValueMap.size(), 5);
        Assert.assertEquals(parsedKeyValueMap.get("a"), "A");
        Assert.assertEquals(parsedKeyValueMap.get("b"), "B");
        Assert.assertEquals(parsedKeyValueMap.get("c"), "C=C=C");
        Assert.assertEquals(parsedKeyValueMap.get("d"), "D");
        Assert.assertEquals(parsedKeyValueMap.get("e"), "E");
    }

}
