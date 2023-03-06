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
package org.ame.presto.csv.protocol;

import com.facebook.presto.spi.SchemaTableName;

import java.io.InputStream;
import java.util.List;
import java.util.regex.Pattern;

public interface ISession
{
    InputStream getInputStream(String path)
            throws Exception;

    List<String> getSchemas(String path)
            throws Exception;

    List<String> getTables(String path, String schema, Pattern tablePattern)
            throws Exception;

    List<SchemaTableName> getSchemaTableNames(String path, String schema, Pattern tablePattern)
            throws Exception;

    void close()
            throws Exception;
}
