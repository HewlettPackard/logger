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
import java.util.List;
import java.util.Set;

public class GrokInputBuilder {

    protected static Logger logger = LoggerFactory.getLogger(Parser.class);
    private static final HashSet<String> validParams = getValidParams();
    private String grokPattern;
    private List<JSONObject> mappings;
    private List<JSONObject> innerGroks;
    private String source;

    private static HashSet<String> getValidParams() {
        HashSet<String> params = new HashSet<>();
        params.add("pattern");
        params.add("mappings");
        params.add("groks");
        params.add("source");

        return params;
    }

    public void setParams(JSONObject parserObject) {
        // Validate if all params are valid
        Set inputParams = parserObject.keySet();
        if(!validParams.containsAll(inputParams)) {
            inputParams.removeAll(validParams);
            logger.error("Invalid params found: {}", inputParams);
        }
        this.grokPattern = (String) parserObject.get("pattern");
        this.mappings = (List<JSONObject>) parserObject.get("mappings");
        this.innerGroks = (List<JSONObject>) parserObject.get("groks");
        this.source = (String) parserObject.get("source");
    }

    public String getGrokPattern() {
        return grokPattern;
    }

    public List<JSONObject> getMappings() {
        return mappings;
    }

    public List<JSONObject> getInnerGroks() {
        return innerGroks;
    }

    public String getSource() {
        return source;
    }

}
