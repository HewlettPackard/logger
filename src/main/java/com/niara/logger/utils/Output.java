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

package com.niara.logger.utils;

import java.io.Serializable;

public class Output implements Serializable {

    private final Object _value;
    private final long _timestamp;

    public Output(final Object value, long timestamp) {
        this._value = value;
        this._timestamp = timestamp;
    }

    public Object value() {
        return _value;
    }

    public long timestamp() {
        return _timestamp;
    }

}
