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
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

public class JsonParser extends Parser {

    private JsonInputBuilder jsonInputBuilder;

    public JsonParser(final String log) {
        super(log);
        jsonInputBuilder = new JsonInputBuilder();
    }

    public void setInputBuilder(JSONObject input) {
        jsonInputBuilder.setParams(input);
    }

    public Parser parse() {
        String message = jsonInputBuilder.getSourceKey();
        final Object parsedTokens = get_value(message);
        if (message != null && message instanceof String) {
            JSONParser jsonParser = new JSONParser();
            try {
                event.putAll((Map<? extends String, ?>) jsonParser.parse((String) parsedTokens));
            } catch (ParseException e) {
                logger.error("Key: {} does not contain a valid JSON string: {}", message, parsedTokens);
            }
        }
        return this;
    }
}
