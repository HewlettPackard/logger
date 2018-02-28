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

import com.niara.logger.utils.GrokHandler;
import com.niara.logger.utils.LoggerConfig;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GrokParserTest {
    private Matcher topMatcher;

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
        GrokHandler.init();
        Parser.init(LoggerConfig.getTimestampKeys(), LoggerConfig.getTimestampDefault());
    }

    @Test
    public void testGrokParser() {
        String log = "foo";
        JSONObject grokObject = new JSONObject();
        String pattern = "%{GREEDYDATA}";
        grokObject.put("pattern", pattern);
        Parser grokParser = new GrokParser(log);
        grokParser.setInputBuilder(grokObject);
        grokParser.parse();
        topMatcher = grokParser.getTopMatcher();

        Assert.assertNotNull(topMatcher);
    }

    @Test
    public void testGrokParserWithNamedGroups() {
        String log = "foo";
        JSONObject grokObject = new JSONObject();
        String pattern = "%{GREEDYDATA:bar}";
        grokObject.put("pattern", pattern);
        Parser grokParser = new GrokParser(log);
        grokParser.setInputBuilder(grokObject);
        grokParser.parse();
        Map<String, Object> parsedMap = grokParser.getEventMap();
        Assert.assertEquals(parsedMap.size(), 1);
        Assert.assertEquals(parsedMap.get("bar"), "foo");
    }

}
