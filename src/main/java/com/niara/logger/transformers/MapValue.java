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
import org.json.simple.JSONObject;


public class MapValue extends Transformer {

    private String key;
    private JSONObject valueMap;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
        this.valueMap = (JSONObject) params.get("map");
    }

    @Override
    public Parser transform(Parser parser) {
        if (valueMap.containsKey(parser.get_value(key))) {
            parser.set_value(key, valueMap.get(parser.get_value(key)));
        } else if (valueMap.containsKey("default")) {
            parser.set_value(key, valueMap.get("default"));
        }
        return parser;
    }

}
