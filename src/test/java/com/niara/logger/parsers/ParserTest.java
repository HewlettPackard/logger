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

import com.niara.logger.utils.Output;
import com.niara.logger.utils.LoggerConfig;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserTest {

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
    public void testToJsonWithNullTopMatcher() {
        String dummyLogLine = "dummy log line";
        Parser csvParser = new CsvParser(dummyLogLine);
        csvParser.event = new HashMap<>();
        csvParser.event.put("timestamp", 0);
        csvParser.event.put("data", "bar\tbaz");
        Output op = csvParser.to_json();
        Assert.assertNull(op.value());
    }

    @Test
    public void testToJsonWithSpecialCharacters() {
        String dummyLogLine = "dummy log line";
        Parser csvParser = new CsvParser(dummyLogLine);
        csvParser.setTopMatcher(this.topMatcher);
        csvParser.event = new TreeMap<>();
        csvParser.event.put("timestamp", 0);
        csvParser.event.put("data", "foo\rbar,foo\tbar,foo\nbar,foo\\bar,foo\"bar");
        Output op = csvParser.to_json();
        String expectedValue = "{\"data\":\"foo\\rbar,foo\\tbar,foo\\nbar,foo\\\\bar,foo\\\"bar\",\"timestamp\":0}";
        String actualValue = op.value().toString();
        Assert.assertEquals(expectedValue, actualValue);
    }


    @Test
    public void testToJsonWithLongValue() {
        String dummyLogLine = "dummy log line";
        Parser csvParser = new CsvParser(dummyLogLine);
        csvParser.setTopMatcher(this.topMatcher);
        csvParser.event = new TreeMap<>();
        csvParser.event.put("timestamp", 0);
        csvParser.event.put("data", 5L);
        Output op = csvParser.to_json();
        String expectedValue = "{\"data\":5,\"timestamp\":0}";
        String actualValue = op.value().toString();
        Assert.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testToJsonWithEmptyList() {
        String dummyLogLine = "check log with special characters";
        Parser csvParser = new CsvParser(dummyLogLine);
        csvParser.setTopMatcher(this.topMatcher);
        csvParser.event = new TreeMap<>();
        csvParser.event.put("timestamp", 0);
        csvParser.event.put("data", new ArrayList<String>());
        Output op = csvParser.to_json();
        String expectedValue = "{\"data\":[],\"timestamp\":0}";
        String actualValue = op.value().toString();
        Assert.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testToJsonWithNonEmptyList() {
        String dummyLogLine = "check log with special characters";
        Parser csvParser = new CsvParser(dummyLogLine);
        csvParser.setTopMatcher(this.topMatcher);
        csvParser.event = new TreeMap<>();
        csvParser.event.put("timestamp", 0);
        List<String> values = new ArrayList<>();
        values.add("one");
        values.add("two");
        csvParser.event.put("data", values);
        Output op = csvParser.to_json();
        String expectedValue = "{\"data\":[\"one\",\"two\"],\"timestamp\":0}";
        String actualValue = op.value().toString();
        Assert.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testToJsonWithNullValue() {
        String dummyLogLine = "check log with special characters";
        Parser csvParser = new CsvParser(dummyLogLine);
        csvParser.setTopMatcher(this.topMatcher);
        csvParser.event = new TreeMap<>();
        csvParser.event.put("timestamp", 0);
        csvParser.event.put("data", null);
        Output op = csvParser.to_json();
        String expectedValue = "{\"data\":null,\"timestamp\":0}";
        String actualValue = op.value().toString();
        Assert.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testToJsonWithBooleanValue() {
        String dummyLogLine = "check log with special characters";
        Parser csvParser = new CsvParser(dummyLogLine);
        csvParser.setTopMatcher(this.topMatcher);
        csvParser.event = new TreeMap<>();
        csvParser.event.put("timestamp", 0);
        csvParser.event.put("data", true);
        Output op = csvParser.to_json();
        String expectedValue = "{\"data\":true,\"timestamp\":0}";
        String actualValue = op.value().toString();
        Assert.assertEquals(expectedValue, actualValue);
    }

}