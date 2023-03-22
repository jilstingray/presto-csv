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
package org.ame.presto.csv;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.ame.presto.csv.description.CSVColumnDescription;
import org.ame.presto.csv.description.CSVTableDescription;
import org.ame.presto.csv.description.CSVTableDescriptionSupplier;
import org.ame.presto.csv.session.ISession;
import org.ame.presto.csv.session.SessionProvider;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CSVClient
{
    private final CSVConfig config;
    private Map<SchemaTableName, CSVTableDescription> tableDescriptions;

    @Inject
    public CSVClient(CSVConfig config, JsonCodec<Map<String, List<CSVTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        this.config = config;
        fetchDescriptions();
    }

    public String getDelimiter(SchemaTableName schemaTableName)
    {
        fetchDescriptions();
        return tableDescriptions.get(schemaTableName).getDelimiter();
    }

    public Boolean getHasHeader(SchemaTableName schemaTableName)
    {
        fetchDescriptions();
        return tableDescriptions.get(schemaTableName).getHasHeader();
    }

    public Map<String, String> getSessionInfo()
    {
        Map<String, String> sessionInfo = new HashMap<>();
        sessionInfo.put("base", config.getBase());
        sessionInfo.put("protocol", config.getProtocol());
        sessionInfo.put("host", config.getHost());
        sessionInfo.put("port", config.getPort().toString());
        sessionInfo.put("username", config.getUsername());
        sessionInfo.put("password", config.getPassword());
        return sessionInfo;
    }

    public List<String> getSchemaNames()
    {
        fetchDescriptions();
        List<String> schemas = new ArrayList<>();
        for (SchemaTableName schemaTableName : tableDescriptions.keySet()) {
            if (!schemas.contains(schemaTableName.getSchemaName())) {
                schemas.add(schemaTableName.getSchemaName());
            }
        }
        return ImmutableList.copyOf(schemas);
    }

    public List<String> getTableNames(String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        fetchDescriptions();
        List<String> tables = new ArrayList<>();
        for (SchemaTableName schemaTableName : tableDescriptions.keySet()) {
            if (schemaTableName.getSchemaName().equals(schemaName)) {
                tables.add(schemaTableName.getTableName());
            }
        }
        return ImmutableList.copyOf(tables);
    }

    public Optional<CSVTable> getTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        fetchDescriptions();
        CSVTableDescription tableDescription = tableDescriptions.get(schemaTableName);
        if (tableDescription == null) {
            return Optional.empty();
        }
        List<CSVColumn> columns = new ArrayList<>();
        for (CSVColumnDescription column : tableDescription.getColumns()) {
            columns.add(new CSVColumn(column.getName(), column.getType()));
        }
        return Optional.of(new CSVTable(tableName, columns));
    }

    private void fetchDescriptions()
    {
        JsonCodec<CSVTableDescription> codec = JsonCodec.jsonCodec(CSVTableDescription.class);
        try {
            ISession session = new SessionProvider(getSessionInfo()).getSession();
            CSVTableDescriptionSupplier supplier = new CSVTableDescriptionSupplier(config, codec, session);
            this.tableDescriptions = supplier.get();
            session.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
