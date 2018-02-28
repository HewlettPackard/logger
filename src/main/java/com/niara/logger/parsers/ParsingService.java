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

import com.niara.logger.handlers.LogParserToLogWriter;
import com.niara.logger.utils.Output;
import com.niara.logger.exceptions.GrokException;
import com.niara.logger.exceptions.InvalidTimestampException;
import com.niara.logger.exceptions.MatchDropConditionException;
import com.niara.logger.exceptions.UnsupportedConditionException;
import com.niara.logger.transformers.Transformer;
import com.niara.logger.transformers.TransformerFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParsingService extends LogParser {

    private static final ParserFactory parserFactory = new ParserFactory();
    private static Logger logger = LoggerFactory.getLogger(ParsingService.class);

    // Constructor used in testing
    public ParsingService(String inputLogType) {
        super(inputLogType, null, null);
    }

    public ParsingService(String inputLogType, ArrayList<String> log, LogParserToLogWriter logParserToLogWriter) throws Exception {
        super(inputLogType, log, logParserToLogWriter);
    }

    public void applyTransformers(String key, JSONArray transformers, Parser parser) throws Exception {
        for(int i = 0; i < transformers.size(); i++) {
            JSONObject transformerObj = (JSONObject) transformers.get(i);
            String transformerName = (String) transformerObj.get("transformer");
            JSONObject parameters = (JSONObject) transformerObj.get("params");

            Transformer transformer = TransformerFactory.getTransformer(transformerName);
            transformer.setParams(key, parameters);
            transformer.transform(parser);
        }
    }

    public void processMappings(Parser parser, JSONArray mappings, String prefix, List<String> projectKeys, int level) throws Exception {
        for (int i = 0; i < mappings.size(); i++) {
            JSONObject mapping = (JSONObject) mappings.get(i);
            String sourceField = (String) mapping.get("source");
            String targetField = (String) mapping.get("target");
            String targetFieldType = (String) mapping.get("type");
            JSONArray transformers = (JSONArray) mapping.get("transformers");
            if(level == 0) {
                projectKeys.add(targetField);
            }
            if (targetFieldType != null && targetFieldType.equals("object")) {
                JSONObject object = (JSONObject) mapping.get("object");
                prefix += targetField + ' ';
                processMappings(parser, (JSONArray) object.get("mappings"), prefix, projectKeys, level + 1);
                int prefixIndex = prefix.indexOf(targetField + ' ');
                if (prefixIndex != -1) {
                    prefix = prefix.substring(0, prefixIndex);
                }
            } else {
                if (sourceField == null) {
                    if (targetField != null) {
                        if (targetFieldType.equals("integer")) {
                            parser.insert(prefix + targetField, Integer.parseInt(mapping.get("value").toString()));
                        } else {
                            parser.insert(prefix + targetField, mapping.get("value"));
                        }
                    }
                } else {
                    parser.copy(Arrays.asList(sourceField), Arrays.asList(prefix + targetField));
                }
                if (transformers != null) {
                    applyTransformers(prefix + targetField, transformers, parser);
                }
            }
        }
    }

    private boolean matchDropCondition(Parser parser, JSONObject dropCondition) {
        if (dropCondition == null)
            return false;

        List<JSONObject> matchConditions = (List<JSONObject>) dropCondition.get("match");
        for (JSONObject matchCondition: matchConditions) {
            String sourceKey = (String) matchCondition.get("source");
            String sourceValue = (String) parser.get_value(sourceKey);
            if (sourceValue != null) {
                String condition = matchCondition.get("not_in") == null ? "in" : "not_in";
                JSONObject matchingCondition = (JSONObject) matchCondition.get(condition);
                boolean regex = matchingCondition.containsKey("regex") ? (boolean) matchingCondition.get("regex") : false;
                List<String> values = (List<String>) matchingCondition.get("values");

                if (condition.equals("in")) {
                    return matchDropConditionIn(regex, sourceValue, values);
                } else {
                    return !matchDropConditionIn(regex, sourceValue, values);
                }
            }
        }

        return false;
    }

    private boolean matchDropConditionIn(boolean regex, String sourceValue, List<String> values) {
        if(regex) {
            for(String value : values) {
                Pattern p = Pattern.compile(value);
                Matcher m = p.matcher(sourceValue);
                if (m.find()) {
                    return true;
                }
            }
        } else {
            for (String value: values) {
                if (sourceValue.equals(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    public Output parse(String log) throws Exception {
        List<JSONObject> groks = (List<JSONObject>) parsingConfig.get("groks");
        List<JSONObject> parsers = (List<JSONObject>) parsingConfig.get("parsers");
        Parser parser = null;
        Matcher matcher = null;
        Map<String, Object> eventMap = null;
        if (groks != null) {
            for (JSONObject grokObject : groks) {
                parser = new GrokParser(log);
                parser.setInputBuilder(grokObject);
                parser.parse();
                eventMap = parser.getEventMap();
                matcher = parser.getTopMatcher();
                if (eventMap != null) {
                    List<JSONObject> mappings = (List<JSONObject>) grokObject.get("mappings");
                    if (mappings != null) {
                        for (JSONObject mapping : mappings) {
                            String sourceKey = (String) mapping.get("source");
                            String targetKey = (String) mapping.get("target");
                            if (targetKey == null) {
                                targetKey = sourceKey;
                            } else {
                                if (sourceKey == null)
                                  parser.set_value(targetKey, mapping.get("value"));
                                else
                                  parser.set_value(targetKey, parser.get_value(sourceKey));
                            }
                            JSONArray transformers = (JSONArray) mapping.get("transformers");
                            if (transformers != null) {
                                applyTransformers(targetKey, transformers, parser);
                            }
                        }
                    }
                    break;
                }
            }
            // Indicates grok parsing failed
            if (eventMap == null &&  matcher == null) {
                throw new GrokException("Grok matching failed with grok pattern(s): " + Arrays.toString(groks.toArray()));
            }
            if (matchDropCondition(parser, (JSONObject) parsingConfig.get("drop"))) {
                throw new MatchDropConditionException("Matched global drop condition after Grokking for inputLogType: " + inputLogType);
            }
        }
        if (parsers != null) {
            for (JSONObject parserObject : parsers) {
                String parserType = (String) parserObject.get("type");
                parser = parserFactory.getParser(parserType, log);
                if (eventMap != null) {
                    parser.copy(eventMap);
                    parser.copy(matcher);

                    String source = (String) ((JSONObject)parserObject.get("params")).get("source");
                    if(!eventMap.containsKey(source)) {
                        logger.error("source {} does not exist for log_type: {}", source, inputLogType);
                        throw new Exception(String.format("source '%s' does not exist for log_type: '%s'.", source, inputLogType));
                    }
                }
                parser.setInputBuilder(parserObject);
                parser.parse();
                JSONArray transformers = (JSONArray) parserObject.get("transformers");
                if (transformers != null) {
                    String key = parserObject.get("source") == null ? "" : (String) parserObject.get("source");
                    applyTransformers(key, transformers, parser);
                }
                eventMap = parser.getEventMap();
                matcher = parser.getTopMatcher();
            }
        }

        List<String> projectKeys = new ArrayList<>();

        JSONObject conditionalMappings = (JSONObject) parsingConfig.get("conditional_mappings");
        JSONObject dropCondition = (JSONObject) parsingConfig.get("drop");
        if (conditionalMappings != null) {
            JSONArray conditions = (JSONArray) conditionalMappings.get("conditions");
            for(int i = 0; i < conditions.size(); i++) {
                String condition = (String) conditions.get(i);
                JSONObject userConditions = (JSONObject) parsingConfig.get(condition);
                String parsedCondition = parser.get_value(condition).toString();
                JSONObject options = (JSONObject) userConditions.get(parsedCondition);
                if (options == null) {
                    throw new UnsupportedConditionException("Unsupported condition: " + parsedCondition + " seen for inputLogType: " + inputLogType);
                }
                if (matchDropCondition(parser, dropCondition)) {
                    throw new MatchDropConditionException("Matched global drop condition for inputLogType: " + inputLogType);
                }
                processMappings(parser, (JSONArray) options.get("mappings"), "", projectKeys, 0);
                JSONObject dropWithinConditionalMapping = (JSONObject) options.get("drop");
                if (dropWithinConditionalMapping != null) {
                    if (matchDropCondition(parser, dropWithinConditionalMapping)) {
                        throw new MatchDropConditionException("Matched drop condition for condition " + parsedCondition + " of inputLogType: " + inputLogType);
                    }
                }

            }
        }
        if (parsingConfig.get("mappings") != null) {
            // Process the mappings directly if exists.
            if (matchDropCondition(parser, dropCondition)) {
                throw new MatchDropConditionException("Matched global drop condition for inputLogType: " + inputLogType);
            }
            processMappings(parser, (JSONArray) parsingConfig.get("mappings"), "", projectKeys, 0);
        }
        List<String> discardKeys = (List<String>) parsingConfig.get("discard_fields");
        if (discardKeys != null) {
            parser = parser.discard(discardKeys);
        } else {
            if (projectKeys.size() > 0)
                parser = parser.project(projectKeys);
        }

        Output parsedOutput = parser.to_json();
        if (parsedOutput.timestamp() == 0)
            throw new InvalidTimestampException("Invalid Timestamp value: " + parsedOutput.timestamp());

        return parsedOutput;
    }

}