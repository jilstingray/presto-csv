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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class CSVColumnDescription
{
    private final String name;
    private final Type type;

    @JsonCreator
    public CSVColumnDescription(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        if (isNullOrEmpty(type)) {
            this.type = VarcharType.VARCHAR;
            return;
        }
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                this.type = BooleanType.BOOLEAN;
                break;
            case "BIGINT":
                this.type = BigintType.BIGINT;
                break;
            case "DOUBLE":
                this.type = DoubleType.DOUBLE;
                break;
            default:
                this.type = VarcharType.VARCHAR;
        }
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .toString();
    }
}
