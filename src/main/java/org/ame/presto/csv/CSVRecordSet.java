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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class CSVRecordSet
        implements RecordSet
{
    private final List<CSVColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final SchemaTableName schemaTableName;
    private final Map<String, String> sessionInfo;
    private final String delimiter;

    public CSVRecordSet(CSVSplit split, List<CSVColumnHandle> columnHandles)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnHandles.stream().map(CSVColumnHandle::getColumnType).collect(Collectors.toList());
        requireNonNull(split, "split is null");
        schemaTableName = new SchemaTableName(split.getSchemaName(), split.getTableName());
        sessionInfo = split.getSessionInfo();
        delimiter = split.getDelimiter();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        try {
            return new CSVRecordCursor(columnHandles, schemaTableName, sessionInfo, delimiter);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
