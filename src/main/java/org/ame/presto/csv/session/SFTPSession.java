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
import com.jcraft.jsch.Session;
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
    private Session session;
    private ChannelSftp channel;

    public SFTPSession(Map<String, String> sessionInfo)
            throws Exception
    {
        this.base = sessionInfo.get("base");
        if (base.endsWith("/") || base.endsWith("\\")) {
            base = base.substring(0, base.length() - 1);
        }
        this.host = sessionInfo.get("host");
        this.port = Integer.parseInt(sessionInfo.get("port"));
        this.username = sessionInfo.get("username");
        this.password = sessionInfo.get("password");
        session = new JSch().getSession(username, host, port);
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(TIMEOUT);
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect(TIMEOUT);
    }

    @Override
    public InputStream getInputStream(String schemaName, String tableName)
            throws SftpException
    {
        InputStream inputStream = channel.get(base + "/" + schemaName + "/" + tableName);
        return inputStream;
    }

    @Override
    public List<SchemaTableName> getSchemaTableNames(String schemaName, String tableName, boolean wildcard)
            throws SftpException
    {
        List<SchemaTableName> schemaTableNames = new ArrayList<>();
        List<String> tables;
        if (wildcard) {
            tables = getTables(schemaName, Pattern.compile(tableName));
        }
        else {
            tables = getTables(schemaName, tableName);
        }
        for (String table : tables) {
            schemaTableNames.add(new SchemaTableName(schemaName, table));
        }
        return schemaTableNames;
    }

    @Override
    public void close()
    {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    private List<String> getTables(String schemaName, Pattern tableName)
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

    private List<String> getTables(String schemaName, String tableName)
            throws SftpException
    {
        List<String> tables = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(base + "/" + schemaName);
        for (ChannelSftp.LsEntry entry : entries) {
            if (!entry.getAttrs().isDir() && tableName.equals(entry.getFilename())) {
                tables.add(entry.getFilename());
                break;
            }
        }
        return tables;
    }
}
