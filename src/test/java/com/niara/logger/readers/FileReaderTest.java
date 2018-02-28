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

package com.niara.logger.readers;


import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Tuple;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

public class FileReaderTest {

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
    }

    @Test
    public void testReadOverMaxLinesPerBatch() throws Exception {
        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("logger-", "-reader");
            OutputStreamWriter fileWriter = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8);
            for (int i = 0; i < LoggerConfig.getMaxLinesPerBatch() + 1; i++) {
                fileWriter.write("This is a sample test log line");
                fileWriter.write("\n");
            }
            fileWriter.close();
            tmpFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }

        FileReader fileReader = new FileReader("dummy_log_type", tmpFile.toPath());
        Tuple<Long, String> readInfo = fileReader.read();
        Assert.assertEquals(readInfo.x, new Long(LoggerConfig.getMaxLinesPerBatch() + 1));
        Assert.assertEquals(readInfo.y, "dummy_log_type");
    }

    @Test
    public void testReadWithWhitespacesInFile() throws Exception {
        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("logger-whitespace-", "-reader");
            OutputStreamWriter fileWriter = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8);
            for (int i = 0; i < 5; i++) {
                fileWriter.write("       ");
                fileWriter.write("\n");
            }
            fileWriter.close();
            tmpFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }

        FileReader fileReader = new FileReader("dummy_log_type", tmpFile.toPath());
        Tuple<Long, String> readInfo = fileReader.read();
        Assert.assertEquals(readInfo.x, new Long(0));
        Assert.assertEquals(readInfo.y, "dummy_log_type");
    }

}
