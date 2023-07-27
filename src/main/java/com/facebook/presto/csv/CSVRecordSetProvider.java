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
package com.facebook.presto.csv;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.csv.session.ISession;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CSVRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger logger = Logger.get(CSVRecordSetProvider.class);
    private final CSVClient csvClient;

    @Inject
    public CSVRecordSetProvider(CSVClient csvClient)
    {
        this.csvClient = requireNonNull(csvClient, "csvClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        CSVSplit csvSplit = (CSVSplit) split;
        List<CSVColumnHandle> handles = columns.stream().map(c -> (CSVColumnHandle) c).collect(toList());
        List<Integer> columnIndexes = handles.stream().map(CSVColumnHandle::getOrdinalPosition).collect(toList());
        ISession iSession = csvClient.getSession();
        try {
            InputStream inputStream = iSession.getInputStream(csvSplit.getSchemaName(), csvSplit.getTableName());
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            Stream<List<?>> stream = reader.lines().skip(1).map(line -> {
                String[] values = line.split(csvClient.getSplitter(), -1);
                return handles.stream().map(handle -> {
                    int index = handle.getOrdinalPosition();
                    if (index >= values.length) {
                        return null;
                    }
                    return values[index];
                }).collect(toList());
            });
            Iterable<List<?>> rows = stream::iterator;
            Iterable<List<?>> mappedRows = Iterables.transform(rows, row -> columnIndexes
                    .stream()
                    .map(row::get)
                    .collect(toList()));
            List<Type> mappedTypes = handles
                    .stream()
                    .map(CSVColumnHandle::getColumnType)
                    .collect(toList());
            return new InMemoryRecordSet(mappedTypes, mappedRows);
        }
        catch (Exception e) {
            logger.error(e, "Error while reading csv file: %s", csvSplit.getTableName());
        }
        return null;
    }
}
