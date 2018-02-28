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


import org.json.simple.JSONObject;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GrokParser extends Parser {

    private GrokInputBuilder grokInputBuilder;

    public GrokParser(final String log) {
        super(log);
        grokInputBuilder = new GrokInputBuilder();
    }

    public void setInputBuilder(JSONObject input) {
        grokInputBuilder.setParams(input);
    }

    public Parser parse() {
        String grokPattern = grokInputBuilder.getGrokPattern();
        final Pattern pattern = grok2pattern(grokPattern);
        Matcher tmp = null;
        if (pattern != null) {
            tmp = pattern.matcher(log);
            if (tmp.find()) {
                this.topMatcher = tmp;
                // logger.debug("if_grok found.");
                // Initialize event map now, unless any of if/else_if are true, all operations are null op.
                this.event = new HashMap<>();
                // We can now populate event with the named groups and their values
                List<String> namedGroups = getNamedGroups(grokPattern);
                if (namedGroups !=null) {
                    for(String namedGroup: namedGroups) {
                        try {
                            String namedGroupValue = tmp.group(namedGroup);
                            this.event.put(namedGroup, (String)namedGroupValue);
                        } catch(IllegalArgumentException e) {
                            logger.error("named groups: {}", namedGroups);
                            logger.error("log: {}", log);
                            logger.error("Cannot match named groups with log line");
                            break;
                        }
                    }
                }
                logger.debug("Grok Processing: {}", Arrays.toString(this.event.entrySet().toArray()));

                // now go through child grok objects
                List<JSONObject> childGroks = grokInputBuilder.getInnerGroks();
                if (childGroks!=null) {
                    List<String> parsedCaptureGroups = new ArrayList<String>();

                    for (JSONObject childGrok: childGroks) {
                        Map<String, Object> childGrokEventMap = null;

                        // any child grok object must have a source
                        String sourceKey = (String) childGrok.get("source");
                        if (sourceKey == null) {
                            logger.error("inner grok object: {}", childGrok);
                            logger.error("missing source key in inner grok");
                            continue;
                        }
                        if (!this.event.containsKey(sourceKey)) {
                            logger.error("inner grok object: {}", childGrok);
                            logger.error("Undefined source key:{}", sourceKey);
                            continue;
                        }
                        if (parsedCaptureGroups.contains(sourceKey)) {
                            // a child grok object with this source was already successful
                            continue;
                        }
                        Parser childGrokParser = new GrokParser((String) this.event.get(sourceKey));
                        childGrokParser.setInputBuilder(childGrok);
                        childGrokParser.parse();
                        childGrokEventMap = childGrokParser.getEventMap();

                        // inner groks should not have mappings
                        if (childGrokEventMap != null) {
                            // Indicates that childGrokObject was successful
                            // We copy child eventMap onto the parent eventMap
                            copy(childGrokEventMap);
                            // this is the first childGrok object with this source
                            parsedCaptureGroups.add(sourceKey);
                        }
                    }
                }
            } else {
                logger.debug("Grok pattern: {} failed to match log line: {}", grokPattern, log);
            }
        } else {
            this.topMatcher = null;
            logger.debug("Could not parse Grok pattern: {}", grokPattern);
        }


        return this;
    }

}
