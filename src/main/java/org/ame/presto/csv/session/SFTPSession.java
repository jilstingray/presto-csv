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
package org.ame.presto.csv.session;

import com.facebook.presto.spi.SchemaTableName;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SFTPSession
        implements ISession
{
    private static final Integer TIMEOUT = 10000;
    private String base;
    private String host;
    private int port;
    private String username;
    private String password;
    private ChannelSftp channel;

    public SFTPSession(Map<String, String> sessionInfo)
            throws Exception
    {
        this.base = sessionInfo.get("base");
        if (base.endsWith("/") || base.endsWith("\\")) {
            base = base.substring(0, base.length() - 1);
        }
        if (!base.startsWith("/") || !base.startsWith("\\")) {
            base = "/" + base;
        }
        this.host = sessionInfo.get("host");
        this.port = Integer.parseInt(sessionInfo.get("port"));
        this.username = sessionInfo.get("username");
        this.password = sessionInfo.get("password");
        JSch jsch = new JSch();
        com.jcraft.jsch.Session session = jsch.getSession(username, host, port);
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(TIMEOUT);
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();
    }

    @Override
    public InputStream getInputStream(String schemaName, String tableName)
            throws SftpException
    {
        InputStream inputStream = channel.get(base + "/" + schemaName + "/" + tableName);
        return inputStream;
    }

    @Override
    public List<String> getSchemas()
            throws SftpException
    {
        List<String> schemas = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(base);
        for (ChannelSftp.LsEntry entry : entries) {
            if (entry.getAttrs().isDir()) {
                schemas.add(entry.getFilename());
            }
        }
        return schemas;
    }

    @Override
    public List<String> getTables(String schemaName, Pattern tableName)
            throws SftpException
    {
        List<String> tables = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(base + "/" + schemaName);
        for (ChannelSftp.LsEntry entry : entries) {
            if (!entry.getAttrs().isDir() && tableName.matcher(entry.getFilename()).find()) {
                tables.add(entry.getFilename());
            }
        }
        return tables;
    }

    @Override
    public List<SchemaTableName> getSchemaTableNames(String schemaName, Pattern tableName)
            throws Exception
    {
        List<String> tables = getTables(schemaName, tableName);
        List<SchemaTableName> schemaTableNames = new ArrayList<>();
        for (String table : tables) {
            schemaTableNames.add(new SchemaTableName(schemaName, table));
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
