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

package com.niara.logger.transformers;

import com.niara.logger.parsers.Parser;
import com.niara.logger.parsers.ParserFactory;
import com.niara.logger.utils.LoggerConfig;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bvarghese on 6/13/17.
 */
public class StringToDateTest {

    private Parser parser;
    private ParserFactory parserFactory;
    private Transformer transformer;
    private TransformerFactory transformerFactory;

    @BeforeClass
    public void init() throws Exception {
        LoggerConfig.init("TEST");
        transformerFactory = new TransformerFactory();
        transformer = transformerFactory.createTransformer("string_to_date");
        parserFactory = new ParserFactory();
        parser = parserFactory.getParser("CsvParser", "test log line");
        Pattern pattern = Pattern.compile(".*");
        Matcher matcher = pattern.matcher("test string");
        parser.setTopMatcher(matcher);
    }

    @Test
    public void testStringToDateInISOFormatWithSeconds() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("date_field", "2017-09-07T01:01:01");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        JSONArray formats = new JSONArray();
        formats.add("yyyy-MM-dd'T'HH:mm:ss");
        params.put("formats", formats);
        transformer.setParams("date_field", params);
        transformer.transform(parser);

        Date date = (Date) parser.get_value("date_field");
        Assert.assertEquals(date.getTime(), 1504771261000L);
    }

    @Test
    public void testStringToDateInISOFormatWithSecondsAndTimeZone() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("date_field", "2017-09-07T01:01:01");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        JSONArray formats = new JSONArray();
        formats.add("yyyy-MM-dd'T'HH:mm:ss");
        params.put("formats", formats);
        params.put("timezone", "UTC");
        transformer.setParams("date_field", params);
        transformer.transform(parser);

        Date date = (Date) parser.get_value("date_field");
        Assert.assertEquals(date.getTime(), 1504746061000L);
    }

    @Test
    public void testStringToDateInISOFormatWithMilliSeconds() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("date_field", "2017-09-07T01:01:01.999");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        JSONArray formats = new JSONArray();
        formats.add("yyyy-MM-dd'T'HH:mm:ss.SSS");
        params.put("formats", formats);
        transformer.setParams("date_field", params);
        transformer.transform(parser);

        Date date = (Date) parser.get_value("date_field");
        Assert.assertEquals(date.getTime(), 1504771261999L);
    }

    @Test
    public void testStringToDateInISOFormatWithMicroSeconds() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("date_field", "2017-09-07T01:01:01.999999");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        JSONArray formats = new JSONArray();
        formats.add("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        params.put("formats", formats);
        transformer.setParams("date_field", params);
        transformer.transform(parser);

        Date date = (Date) parser.get_value("date_field");
        // Currently, we don't deal with MicroSeconds
        Assert.assertEquals(date.getTime(), 1504771261000L);
    }

}
