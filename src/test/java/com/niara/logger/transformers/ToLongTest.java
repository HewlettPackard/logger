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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bvarghese on 6/13/17.
 */
public class ToLongTest {

    private Parser parser;
    private ParserFactory parserFactory;
    private Transformer transformer;
    private TransformerFactory transformerFactory;

    @BeforeClass
    public void init() throws Exception {
        LoggerConfig.init("TEST");
        transformerFactory = new TransformerFactory();
        transformer = transformerFactory.createTransformer("to_long");
        parserFactory = new ParserFactory();
        parser = parserFactory.getParser("CsvParser", "test log line");
        Pattern pattern = Pattern.compile(".*");
        Matcher matcher = pattern.matcher("test string");
        parser.setTopMatcher(matcher);
    }

    @Test
    public void testToLongWithDoubleValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("test", 200.98);
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        transformer.setParams("test", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("test"), new Long(201));
    }

    @Test
    public void testToLongWithIntegerValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("test", 200);
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        transformer.setParams("test", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("test"), new Long(200));
    }

    @Test
    public void testToLongWithLongValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("test", 5000000000L);
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        transformer.setParams("test", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("test"), new Long(5000000000L));
    }

    @Test
    public void testToLongWithStringValue() {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("test", "200");
        parser.setEvent(eventMap);
        JSONObject params = new JSONObject();
        transformer.setParams("test", params);
        transformer.transform(parser);

        Assert.assertEquals(parser.get_value("test"), new Long(200));
    }

}
