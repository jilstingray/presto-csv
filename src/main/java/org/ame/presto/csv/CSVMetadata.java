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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CSVMetadata
        implements ConnectorMetadata
{
    private final CSVClient csvClient;

    @Inject
    public CSVMetadata(CSVClient client)
    {
        this.csvClient = client;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return csvClient.getSchemaNames();
    }

    @Override
    public CSVTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!csvClient.getSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }
        if (!csvClient.getTableNames(tableName.getSchemaName()).contains(tableName.getTableName())) {
            return null;
        }
        return new CSVTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        CSVTableHandle tableHandle = (CSVTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new CSVTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        Optional<ConnectorTableMetadata> connectorTableMetadata = getTableMetadata(session, ((CSVTableHandle) table).getSchemaTableName());
        if (!connectorTableMetadata.isPresent()) {
            throw new RuntimeException("Table not found: " + table);
        }
        return connectorTableMetadata.get();
    }

    private Optional<ConnectorTableMetadata> getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        if (!listSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            return Optional.empty();
        }
        Optional<CSVTable> table = csvClient.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        return table.map(csvTable -> new ConnectorTableMetadata(schemaTableName, csvTable.getColumnsMetadata()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CSVTableHandle excelTableHandle = (CSVTableHandle) tableHandle;
        Optional<CSVTable> table = csvClient.getTable(excelTableHandle.getSchemaName(), excelTableHandle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(excelTableHandle.getSchemaTableName());
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int i = 0;
        for (ColumnMetadata column : table.get().getColumnsMetadata()) {
            columnHandles.put(column.getName(), new CSVColumnHandle(column.getName(), column.getType(), i++));
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((CSVColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, Optional.of(prefix.getSchemaName()))) {
            Optional<ConnectorTableMetadata> tableMetadata = getTableMetadata(session, tableName);
            // table can disappear during listing operation
            tableMetadata.ifPresent(connectorTableMetadata -> columns.put(tableName, connectorTableMetadata.getColumns()));
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<String> schemaListBuilder = ImmutableList.builder();
        ImmutableList.Builder<SchemaTableName> tableListBuilder = ImmutableList.builder();
        if (schemaName.isPresent()) {
            schemaListBuilder.add(schemaName.get());
        }
        else {
            schemaListBuilder.addAll(listSchemaNames(session));
        }
        ImmutableList<String> schemaList = schemaListBuilder.build();
        for (String schema : schemaList) {
            tableListBuilder.addAll(listTables(schema));
        }
        return tableListBuilder.build();
    }

    private List<SchemaTableName> listTables(String schemaName)
    {
        return csvClient.getTableNames(schemaName).stream()
                .map(tableName -> new SchemaTableName(schemaName, tableName))
                .collect(toImmutableList());
    }
}
