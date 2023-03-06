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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.ame.presto.csv.CSVConnectorConfig;
import org.ame.presto.csv.protocol.ISession;

import javax.inject.Inject;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class CSVTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, CSVTableDescription>>
{
    private static final Logger log = Logger.get(CSVTableDescriptionSupplier.class);
    private final CSVConnectorConfig config;
    private final JsonCodec<CSVTableDescription> codec;
    private final ISession session;

    @Inject
    public CSVTableDescriptionSupplier(CSVConnectorConfig config, JsonCodec<CSVTableDescription> codec, ISession session)
    {
        this.config = requireNonNull(config);
        this.codec = requireNonNull(codec);
        this.session = session;
    }

    @Override
    public Map<SchemaTableName, CSVTableDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, CSVTableDescription> builder = ImmutableMap.builder();
        List<SchemaTableName> schemaTableNames;
        try {
            for (File file : listFiles(new File(config.getTableDescriptionDir()).toPath().toFile())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    CSVTableDescription table = codec.fromJson(readAllBytes(file.toPath()));
                    // get table names
                    if (session == null) {
                        schemaTableNames = Files.list(new File(config.getBase()).toPath().resolve(table.getSchemaName()))
                                .filter(p -> table.getTableName().matcher(p.getFileName().toString()).find())
                                .map(p -> new SchemaTableName(table.getSchemaName(), p.getFileName().toString()))
                                .collect(Collectors.toList());
                    }
                    else {
                        schemaTableNames = session.getSchemaTableNames(config.getBase(), table.getSchemaName(), table.getTableName());
                    }
                    for (SchemaTableName name : schemaTableNames) {
                        builder.put(name, table);
                    }
                }
            }
            return builder.build();
        }
        catch (Exception e) {
            log.warn(e, "Error: ");
            throw new RuntimeException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
