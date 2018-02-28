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


public class CsvParserTest {

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
    public void testCsvParser() {
        String csvLog = "a,b,\"c c c\",d,e";
        Parser csvParser = new CsvParser(csvLog);
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("csvLog", csvLog);
        csvParser.setEvent(eventMap);
        csvParser.setTopMatcher(this.topMatcher);
        JSONObject parserConfig = new JSONObject();
        JSONObject params = new JSONObject();
        params.put("source", "csvLog");
        params.put("separator", ",");
        List<String> csvFields = new ArrayList<>(Arrays.asList("c1", "c2", "c3", "c4", "c5"));
        params.put("fields", csvFields);
        parserConfig.put("params", params);
        csvParser.setInputBuilder(parserConfig);
        csvParser.parse();
        Map<String, Object> parsedMap = csvParser.getEventMap();
        Assert.assertEquals(parsedMap.size(), 6);
        Assert.assertEquals(parsedMap.get("c1"), "a");
        Assert.assertEquals(parsedMap.get("c2"), "b");
        Assert.assertEquals(parsedMap.get("c3"), "c c c");
        Assert.assertEquals(parsedMap.get("c4"), "d");
        Assert.assertEquals(parsedMap.get("c5"), "e");
        Assert.assertEquals(parsedMap.get("csvLog"), csvLog);
    }

    @Test
    public void testCsvParserWithTargetField() {
        String csvLog = "a,b,\"c c c\",d,e";
        Parser csvParser = new CsvParser(csvLog);
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("csvLog", csvLog);
        csvParser.setEvent(eventMap);
        csvParser.setTopMatcher(this.topMatcher);
        JSONObject parserConfig = new JSONObject();
        JSONObject params = new JSONObject();
        params.put("source", "csvLog");
        params.put("target", "target_field");
        params.put("separator", ",");
        List<String> csvFields = new ArrayList<>(Arrays.asList("c1", "c2", "c3", "c4", "c5"));
        params.put("fields", csvFields);
        parserConfig.put("params", params);
        csvParser.setInputBuilder(parserConfig);
        csvParser.parse();
        Map<String, Object> parsedMap = csvParser.getEventMap();
        Assert.assertEquals(parsedMap.size(), 2);
        Assert.assertEquals(parsedMap.get("csvLog"), csvLog);
        Map<String, Object> parsedCsvMap = (Map<String, Object>) parsedMap.get("target_field");
        Assert.assertEquals(parsedCsvMap.size(), 5);
        Assert.assertEquals(parsedCsvMap.get("c1"), "a");
        Assert.assertEquals(parsedCsvMap.get("c2"), "b");
        Assert.assertEquals(parsedCsvMap.get("c3"), "c c c");
        Assert.assertEquals(parsedCsvMap.get("c4"), "d");
        Assert.assertEquals(parsedCsvMap.get("c5"), "e");
    }

}
