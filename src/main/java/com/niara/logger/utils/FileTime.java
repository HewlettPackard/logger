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

import java.io.File;


public class FileTime implements Comparable {

    private final File file;
    private final long lastModified;

    public FileTime(final File file) {
        this.file = file;
        this.lastModified = file.lastModified();
    }

    public File getFile() {
        return file;
    }

    public long getLastModified() {
        return lastModified;
    }

    @Override
    public int compareTo(Object object) {
        long other = ((FileTime) object).lastModified;
        return lastModified < other ? -1 : lastModified == other ? 0 : 1;
    }

}


