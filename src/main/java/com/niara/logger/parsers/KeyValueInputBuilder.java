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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class KeyValueInputBuilder {

    protected static Logger logger = LoggerFactory.getLogger(Parser.class);

    private String sourceKey;
    private String targetKey;
    private String fieldSeparator;
    private String valueSeparator;
    private String keyPrefix;
    private static final HashSet<String> validParams = getValidParams();

    private static HashSet<String> getValidParams() {
        HashSet<String> params = new HashSet<>();
        params.add("source");
        params.add("target");
        params.add("field_separator");
        params.add("value_separator");
        params.add("prefix");

        return params;
    }

    public void setParams(JSONObject parserObject) {
        // Validate if all params are valid
        JSONObject params = (JSONObject) parserObject.get("params");
        Set inputParams = params.keySet();
        if(!validParams.containsAll(inputParams)) {
            inputParams.removeAll(validParams);
            logger.error("Invalid params found: {}", inputParams);
        }
        this.sourceKey = params.get("source") == null ? "" : (String) params.get("source");
        this.targetKey = (String) params.get("target");
        this.fieldSeparator = params.get("field_separator") == null ? "" : (String) params.get("field_separator");
        this.valueSeparator = params.get("value_separator") == null ? "" : (String) params.get("value_separator");
        this.keyPrefix = params.get("prefix") == null ? "" : (String) params.get("prefix");
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public String getFieldSeparator() {
        return fieldSeparator;
    }

    public String getValueSeparator() {
        return valueSeparator;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }
}
