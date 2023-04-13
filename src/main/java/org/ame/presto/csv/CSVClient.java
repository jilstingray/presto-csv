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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.ame.presto.csv.session.ISession;
import org.ame.presto.csv.session.SessionProvider;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class CSVClient
{
    private static final String IGNORE_QUOTES = "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private final Logger logger = Logger.get(CSVClient.class);
    private final CSVConfig config;
    private final String splitter;
    private final String suffix;

    @Inject
    public CSVClient(CSVConfig config, JsonCodec<Map<String, List<CSVTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        this.config = config;
        this.splitter = config.getSplitter() == null ? "," + IGNORE_QUOTES : config.getSplitter() + IGNORE_QUOTES;
        this.suffix = config.getSuffix() == null ? "csv" : config.getSuffix();
    }

    public List<String> getSchemaNames()
    {
        try {
            ISession session = getSession();
            List<String> schemas = session.getSchemas();
            session.close();
            return ImmutableList.copyOf(schemas);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getTableNames(String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        try {
            ISession session = getSession();
            List<String> tables = session.getTables(schemaName, suffix);
            session.close();
            return ImmutableList.copyOf(tables);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<CSVTable> getTable(String schemaName, String tableName)
    {
        ImmutableList.Builder<CSVColumn> columns = ImmutableList.builder();
        // Assume the first row is always the header
        String[] header;
        Set<String> columnNames = new HashSet<>();
        try {
            ISession session = getSession();
            InputStream inputStream = session.getInputStream(schemaName, tableName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return Optional.empty();
            }
            header = headerLine.split(splitter, -1);
            reader.close();
            inputStream.close();
            session.close();
        }
        catch (Exception e) {
            logger.warn(e, "Error while reading csv file %s", tableName);
            return Optional.empty();
        }
        for (int i = 0; i < header.length; i++) {
            String columnName = header[i].trim();
            // when empty or repeated column header, adding a placeholder column name
            if (columnName.isEmpty() || columnNames.contains(columnName)) {
                columnName = "column_" + i;
            }
            columnNames.add(columnName);
            columns.add(new CSVColumn(columnName, VarcharType.VARCHAR));
        }
        return Optional.of(new CSVTable(tableName, columns.build()));
    }

    public ISession getSession()
    {
        Map<String, String> sessionInfo = new HashMap<>();
        sessionInfo.put("base", config.getBase());
        sessionInfo.put("protocol", config.getProtocol());
        sessionInfo.put("host", config.getHost());
        sessionInfo.put("port", config.getPort().toString());
        sessionInfo.put("username", config.getUsername());
        sessionInfo.put("password", config.getPassword());
        sessionInfo.put("splitter", splitter);
        sessionInfo.put("suffix", suffix);
        return new SessionProvider(sessionInfo).getSession();
    }

    public String getSplitter()
    {
        return splitter;
    }
}
