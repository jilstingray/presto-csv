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
        if (!base.startsWith("/") || !base.startsWith("\\")) {
            base = "/" + base;
        }
    }

    @Override
    public InputStream getInputStream(String schemaName, String tableName)
            throws IOException
    {
        return new File(base + "/" + schemaName + "/" + tableName).toPath().toUri().toURL().openStream();
    }

    @Override
    public List<String> getSchemas()
    {
        List<String> schemas = new ArrayList<>();
        for (File file : listFiles(new File(base).toPath().toFile())) {
            if (file.isDirectory()) {
                schemas.add(file.getName());
            }
        }
        return schemas;
    }

    @Override
    public List<String> getTables(String schemaName, Pattern tableName)
    {
        List<String> tables = new ArrayList<>();
        for (File file : listFiles(new File(base).toPath().resolve(schemaName).toFile())) {
            if (file.isFile() && tableName.matcher(file.getName()).find()) {
                tables.add(file.getName());
            }
        }
        return tables;
    }

    @Override
    public List<SchemaTableName> getSchemaTableNames(String schemaName, Pattern tableName)
    {
        List<String> tables = getTables(schemaName, tableName);
        List<SchemaTableName> schemaTableNames = new ArrayList<>();
        for (String table : tables) {
            schemaTableNames.add(new SchemaTableName(schemaName, table));
        }
        return schemaTableNames;
    }

    @Override
    public void close()
            throws Exception
    {
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
