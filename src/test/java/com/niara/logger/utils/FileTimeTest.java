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

import java.io.File;
import java.io.IOException;


public class FileTimeTest {

    @Test
    public void testFileTime() {
        File oldFile = null;
        File newFile = null;
        try {
            oldFile = File.createTempFile("logger-", "-old-file");
            newFile = File.createTempFile("logger-", "-new-file");
            oldFile.deleteOnExit();
            newFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
        FileTime oldFileTime = new FileTime(oldFile);
        Assert.assertEquals(oldFileTime.getFile(), oldFile);
        Assert.assertEquals(oldFileTime.getLastModified(), oldFile.lastModified());

        newFile.setLastModified(oldFileTime.getLastModified() + 1000);
        FileTime newFileTime = new FileTime(newFile);
        Assert.assertEquals(oldFileTime.compareTo(newFileTime), -1);
        Assert.assertEquals(newFileTime.compareTo(oldFileTime), 1);
        Assert.assertEquals(newFileTime.compareTo(newFileTime), 0);
    }

}
