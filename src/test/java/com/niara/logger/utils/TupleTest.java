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

import org.testng.Assert;
import org.testng.annotations.Test;


public class TupleTest {

    @Test
    public void testTuple() {
        Tuple<String, String> stringTuple = new Tuple<>("first", "second");
        Assert.assertEquals(stringTuple.x, "first");
        Assert.assertEquals(stringTuple.y, "second");
        Assert.assertEquals(stringTuple.getX(), "first");
        Assert.assertEquals(stringTuple.getY(), "second");
        Assert.assertEquals(stringTuple.first(), "first");
        Assert.assertEquals(stringTuple.second(), "second");
    }

}
