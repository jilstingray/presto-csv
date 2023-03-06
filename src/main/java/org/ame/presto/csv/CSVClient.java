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
import org.ame.presto.csv.description.CSVTableDescription;
import org.ame.presto.csv.description.CSVTableDescriptionSupplier;
import org.ame.presto.csv.protocol.ISession;
import org.ame.presto.csv.protocol.SFTPSession;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class CSVClient
{
    private final CSVConnectorConfig config;
    private final String protocol;
    private final SFTPSession sftpSession;
    Map<SchemaTableName, CSVTableDescription> tableDescriptions;

    @Inject
    public CSVClient(CSVConnectorConfig config, JsonCodec<Map<String, List<CSVTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        this.config = config;
        this.protocol = config.getProtocol().toLowerCase(Locale.ENGLISH);
        this.sftpSession = (SFTPSession) getSession();
        JsonCodec<CSVTableDescription> codec = JsonCodec.jsonCodec(CSVTableDescription.class);
        try {
            if (ProtocolType.FILE.toString().equals(protocol)) {
                CSVTableDescriptionSupplier supplier = new CSVTableDescriptionSupplier(config, codec, null);
                this.tableDescriptions = supplier.get();
            }
            else if (sftpSession != null) {
                CSVTableDescriptionSupplier supplier = new CSVTableDescriptionSupplier(config, codec, sftpSession);
                this.tableDescriptions = supplier.get();
            }
            else {
                throw new RuntimeException("Unsupported protocol: " + protocol);
            }
        }
        catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getSchemaNames()
    {
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
        CSVTableDescription tableDescription = tableDescriptions.get(schemaTableName);
        List<List<Object>> values = readAllValues(schemaName, tableName, tableDescription);
        if (tableDescription == null || values.isEmpty()) {
            return Optional.empty();
        }
        List<CSVColumn> columns = new ArrayList<>();
        for (int i = 0; i < tableDescription.getColumns().size(); i++) {
            columns.add(new CSVColumn(tableDescription.getColumns().get(i).getName(), VARCHAR));
        }
        return Optional.of(new CSVTable(tableName, columns, values));
    }

    private List<List<Object>> readAllValues(String schemaName, String tableName, CSVTableDescription tableDescription)
    {
        if (tableDescription == null) {
            return ImmutableList.of();
        }
        try {
            InputStream inputStream = getInputStream(schemaName, tableName);
            List<List<Object>> values = new ArrayList<>();
            try {
                BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(inputStream));
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }
                    String[] fields = line.split(tableDescription.getDelimiter());
                    List<Object> row = new ArrayList<>();
                    for (String field : fields) {
                        if (field.matches("^-?\\d+$")) {
                            row.add(Long.parseLong(field));
                        }
                        else if (field.matches("^-?\\d+\\.\\d+$")) {
                            row.add(Double.parseDouble(field));
                        }
                        else if (field.matches("^-?\\d+\\.\\d+E\\d+$")) {
                            row.add(new BigDecimal(field));
                        }
                        else if (field.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
                            row.add(new SimpleDateFormat("yyyy-MM-dd").parse(field));
                        }
                        else if (field.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")) {
                            row.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(field));
                        }
                        else {
                            row.add(field);
                        }
                    }
                    values.add(row);
                }
            }
            catch (ParseException e) {
                throw new RuntimeException(e);
            }
            inputStream.close();
            return values;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream getInputStream(String schemaName, String tableName)
    {
        String path = config.getBase();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        try {
            if (ProtocolType.FILE.toString().equals(protocol)) {
                Path filePath = new File(path).toPath().resolve(schemaName).resolve(tableName);
                return filePath.toUri().toURL().openStream();
            }
            else if (sftpSession != null) {
                return sftpSession.getInputStream(path + "/" + schemaName + "/" + tableName);
            }
            else {
                throw new RuntimeException("Unsupported protocol: " + protocol);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ISession getSession()
    {
        if (ProtocolType.SFTP.toString().equals(this.protocol)) {
            try {
                return new SFTPSession(config);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }
}
