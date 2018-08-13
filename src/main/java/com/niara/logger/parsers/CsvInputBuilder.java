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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class CsvInputBuilder {

    protected static Logger logger = LoggerFactory.getLogger(Parser.class);
    private static final HashSet<String> validParams = getValidParams();
    private static final List<String> EMPTY_LIST = new ArrayList<String>();
    private static final char DEFAULT_SEPARATOR =  ',';

    private String sourceKey;
    private String targetKey;
    private char fieldSeparator;
    private List<String> csvFields;
    private String[] columnNames;
    private boolean ignoreQuotes;

    private static HashSet<String> getValidParams() {
        HashSet<String> params = new HashSet<>();
        params.add("source");
        params.add("separator");
        params.add("fields");
        params.add("target");

        return params;
    }

    public void setParams(JSONObject parserObject) {
        // Validate if all params are valid
        JSONObject params = (JSONObject) parserObject.get("params");
        Set<String> inputParams = params.keySet();
        if(!validParams.containsAll(inputParams)) {
            inputParams.removeAll(validParams);
            logger.error("Invalid params found: {}", inputParams);
        }
        this.sourceKey = (String) params.get("source");
        this.targetKey = (String) params.get("target");
        this.fieldSeparator = params.get("separator") == null ? DEFAULT_SEPARATOR : ((String) params.get("separator")).charAt(0);
        this.csvFields = params.get("fields") == null ? EMPTY_LIST : (List<String>) params.get("fields");
        this.columnNames = this.csvFields.toArray(new String[csvFields.size()]);
        this.ignoreQuotes = params.get("ignoreQuotes") == null ? true : (boolean) params.get("ignoreQuotes");
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public char getFieldSeparator() {
        return fieldSeparator;
    }

    public List<String> getCsvFields() {
        return csvFields;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public boolean shouldIgnoreQuotes() {
        return ignoreQuotes;
    }

}