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
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.ame.presto.csv.CSVConnectorConfig;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class SFTPSession
        implements ISession
{
    private static final Integer TIMEOUT = 10000;
    private String host;
    private int port;
    private String username;
    private String password;
    private ChannelSftp channel;

    public SFTPSession(CSVConnectorConfig config)
            throws Exception
    {
        this.host = config.getHost();
        this.port = config.getPort();
        this.username = config.getUsername();
        this.password = config.getPassword();
        JSch jsch = new JSch();
        com.jcraft.jsch.Session session = jsch.getSession(username, host, port);
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(TIMEOUT);
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();
    }

    @Override
    public InputStream getInputStream(String path)
            throws SftpException
    {
        InputStream inputStream = channel.get(path);
        return inputStream;
    }

    @Override
    public List<String> getSchemas(String path)
            throws SftpException
    {
        List<String> schemas = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(path);
        for (ChannelSftp.LsEntry entry : entries) {
            if (entry.getAttrs().isDir()) {
                schemas.add(entry.getFilename());
            }
        }
        return schemas;
    }

    @Override
    public List<String> getTables(String path, String schema, Pattern tablePattern)
            throws SftpException
    {
        List<String> tables = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(path + "/" + schema);
        for (ChannelSftp.LsEntry entry : entries) {
            if (!entry.getAttrs().isDir() && tablePattern.matcher(entry.getFilename()).find()) {
                tables.add(entry.getFilename());
            }
        }
        return tables;
    }

    @Override
    public List<SchemaTableName> getSchemaTableNames(String path, String schema, Pattern tablePattern)
            throws Exception
    {
        List<String> tables = getTables(path, schema, tablePattern);
        List<SchemaTableName> schemaTableNames = new ArrayList<>();
        for (String table : tables) {
            schemaTableNames.add(new SchemaTableName(schema, table));
        }
        return schemaTableNames;
    }

    public void close()
            throws JSchException
    {
        channel.disconnect();
        channel.getSession().disconnect();
    }
}
