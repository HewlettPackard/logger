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


public class Replace extends Transformer {

    private String key;
    private String source;
    private String target;
    private boolean isRegex = false;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
        this.source = (String) params.get("source");
        this.target = (String) params.get("target");
        if (params.get("regex") != null) {
            this.isRegex = (boolean) params.get("regex");
        }
    }

    @Override
    public Parser transform(Parser parser) {
        if (parser.get_value(key) == null) {
            logger.debug("Replace transformer failed to transform. Value for Key {} is null", key);
            return parser;
        }
        if (isRegex) {
            return parser.set_value(key, ((String) parser.get_value(key)).replaceAll(source, target));
        } else {
            return parser.set_value(key, ((String) parser.get_value(key)).replace(source, target));
        }
    }

}
