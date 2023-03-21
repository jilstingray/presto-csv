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
package org.ame.presto.csv.description;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class CSVTableDescription
{
    private final String schemaName;
    private final String tableName;
    private final boolean wildcard;
    private final String delimiter;
    private final List<CSVColumnDescription> columns;

    @JsonCreator
    public CSVTableDescription(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("wildcard") boolean wildcard,
            @JsonProperty("delimiter") String delimiter,
            @JsonProperty("columns") List<CSVColumnDescription> columns)

    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.wildcard = !isNullOrEmpty(String.valueOf(wildcard)) && wildcard;
        this.delimiter = requireNonNull(delimiter, "delimiter is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public boolean getwildcard()
    {
        return wildcard;
    }

    @JsonProperty
    public String getDelimiter()
    {
        return delimiter;
    }

    @JsonProperty
    public List<CSVColumnDescription> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("wildcard", wildcard)
                .add("delimiter", delimiter)
                .add("columns", columns)
                .toString();
    }
}
