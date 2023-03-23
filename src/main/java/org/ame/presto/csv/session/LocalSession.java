/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ame.presto.csv.session;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class LocalSession
        implements ISession
{
    private String base;

    public LocalSession(Map<String, String> sessionInfo)
    {
        this.base = sessionInfo.get("base");
        if (base.endsWith("/") || base.endsWith("\\")) {
            base = base.substring(0, base.length() - 1);
        }
    }

    @Override
    public InputStream getInputStream(String schemaName, String tableName)
            throws IOException
    {
        return new File(base + "/" + schemaName + "/" + tableName).toPath().toUri().toURL().openStream();
    }

    @Override
    public List<SchemaTableName> getSchemaTableNames(String schemaName, String tableName, boolean wildcard)
    {
        List<SchemaTableName> schemaTableNames = new ArrayList<>();
        List<String> tables;
        if (wildcard) {
            tables = getTables(schemaName, Pattern.compile(tableName));
        }
        else {
            tables = getTables(schemaName, tableName);
        }
        for (String table : tables) {
            schemaTableNames.add(new SchemaTableName(schemaName, table));
        }
        return schemaTableNames;
    }

    @Override
    public void close()
    {
    }

    private List<String> getTables(String schemaName, Pattern tableNamePattern)
    {
        List<String> tables = new ArrayList<>();
        for (File file : listFiles(new File(base).toPath().resolve(schemaName).toFile())) {
            if (file.isFile() && tableNamePattern.matcher(file.getName()).find()) {
                tables.add(file.getName());
            }
        }
        return tables;
    }

    private List<String> getTables(String schemaName, String tableName)
    {
        List<String> tables = new ArrayList<>();
        for (File file : listFiles(new File(base).toPath().resolve(schemaName).toFile())) {
            if (file.isFile() && tableName.equals(file.getName())) {
                tables.add(file.getName());
                break;
            }
        }
        return tables;
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
