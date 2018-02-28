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


public class SplitAndTake extends Transformer {

    private String key;
    private String separator;
    private Long pos;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
        this.separator = (String) params.get("separator");
        this.pos = (Long) params.get("take");
    }

    @Override
    public Parser transform(Parser parser) {
        String value = (String) parser.get_value(key);
        if (value != null) {
            String[] parts = value.split(separator);
            String part;
            if (pos.intValue() < parts.length) {
                part = parts[pos.intValue()];
            }
            else {
                logger.debug("Returning whole string");
                part = value;
            }
            parser.set_value(key, part);
        }

        return parser;
    }

}
