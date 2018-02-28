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
import org.json.simple.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bvarghese on 6/13/17.
 */
public class RemoveFieldTest {

    private Parser parser;
    private ParserFactory parserFactory;
    private Transformer transformer;
    private TransformerFactory transformerFactory;

    @BeforeClass
    public void init() throws Exception {
        LoggerConfig.init("TEST");
        transformerFactory = new TransformerFactory();
        transformer = transformerFactory.createTransformer("remove_field");
        parserFactory = new ParserFactory();
        parser = parserFactory.getParser("CsvParser", "test log line");
        Pattern pattern = Pattern.compile(".*");
        Matcher matcher = pattern.matcher("test string");
        parser.setTopMatcher(matcher);
    }

    @Test
    public void testRemoveFieldWithEqualsConditionMatchingValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("field", "remove_value");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        List<String> values = new ArrayList<>();
        values.add("remove_value");
        params.put("values", values);
        transformer.setParams("field", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("field"), null);
    }

    @Test
    public void testRemoveFieldWithEqualsConditionNotMatchingValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("field", "dont_remove_value");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        params.put("condition", "equals");
        List<String> values = new ArrayList<>();
        values.add("remove_value");
        params.put("values", values);
        transformer.setParams("field", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("field"), "dont_remove_value");
    }

    @Test
    public void testRemoveFieldWithEqualsConditionWithRegexMatchingValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("field", "remove_regex_value");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        params.put("condition", "equals");
        List<String> values = new ArrayList<>();
        values.add(".*_regex_.*");
        params.put("values", values);
        params.put("regex", true);
        transformer.setParams("field", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("field"), null);
    }

    @Test
    public void testRemoveFieldWithEqualsConditionWithRegexNotMatchingValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("field", "dont_remove_value");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        params.put("condition", "equals");
        List<String> values = new ArrayList<>();
        values.add(".*_regex_.*");
        params.put("values", values);
        params.put("regex", true);
        transformer.setParams("field", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("field"), "dont_remove_value");
    }

    @Test
    public void testRemoveFieldWithNotEqualsConditionMatchingValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("field", "remove_value");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        params.put("condition", "not equals");
        List<String> values = new ArrayList<>();
        values.add("value");
        params.put("values", values);
        transformer.setParams("field", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("field"), null);
    }

    @Test
    public void testRemoveFieldWithNotEqualsConditionNotMatchingValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("field", "dont_remove_value");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        params.put("condition", "not equals");
        List<String> values = new ArrayList<>();
        values.add("dont_remove_value");
        params.put("values", values);
        transformer.setParams("field", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("field"), "dont_remove_value");
    }

    @Test
    public void testRemoveFieldWithNotEqualsConditionWithRegexMatchingValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("field", "remove_regex_value");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        params.put("condition", "not equals");
        List<String> values = new ArrayList<>();
        values.add(".*_valid_.*");
        params.put("values", values);
        params.put("regex", true);
        transformer.setParams("field", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("field"), null);
    }

    @Test
    public void testRemoveFieldWithNotEqualsConditionWithRegexNotMatchingValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("field", "dont_remove_value");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        params.put("condition", "not equals");
        List<String> values = new ArrayList<>();
        values.add(".*_remove_.*");
        params.put("values", values);
        params.put("regex", true);
        transformer.setParams("field", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("field"), "dont_remove_value");
    }

}
