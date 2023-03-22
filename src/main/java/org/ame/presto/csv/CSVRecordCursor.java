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
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.ame.presto.csv.session.ISession;
import org.ame.presto.csv.session.SessionProvider;

import java.io.BufferedReader;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CSVRecordCursor
        implements RecordCursor
{
    private final List<CSVColumnHandle> columnHandles;
    private long totalBytes;
    private List<String> fields;
    private final String delimiter;
    private final boolean hasHeader;
    private final ISession session;
    private final InputStream inputStream;
    private BufferedReader reader;
    private String currentLine;

    public CSVRecordCursor(
            List<CSVColumnHandle> columnHandles,
            SchemaTableName schemaTableName,
            Map<String, String> sessionInfo,
            String delimiter,
            boolean hasHeader)
            throws Exception
    {
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        session = new SessionProvider(sessionInfo).getSession();
        inputStream = session.getInputStream(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (reader == null) {
            try {
                reader = new BufferedReader(new java.io.InputStreamReader(inputStream));
                // skip header if exists
                if (hasHeader) {
                    reader.readLine();
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {
            currentLine = reader.readLine();
            if (currentLine != null) {
                if (!currentLine.isEmpty()) {
                    fields = Arrays.asList(currentLine.split(delimiter));
                    // replace empty or null values with null
                    Collections.replaceAll(fields, "", null);
                    Collections.replaceAll(fields, "null", null);
                    Collections.replaceAll(fields, "NULL", null);
                    totalBytes += currentLine.getBytes().length;
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return currentLine != null;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(fields.get(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    @Override
    public void close()
    {
        try {
            this.reader.close();
            this.inputStream.close();
            this.session.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }
}
