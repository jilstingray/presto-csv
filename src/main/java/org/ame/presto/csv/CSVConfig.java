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

import com.facebook.airlift.configuration.Config;

public class CSVConfig
{
    private String protocol;
    private String base;
    private String tableDescriptionDir;
    private String username;
    private String password;
    private String host;
    private Integer port;

    public String getProtocol()
    {
        return protocol;
    }

    public String getBase()
    {
        return base;
    }

    public String getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getHost()
    {
        return host;
    }

    public Integer getPort()
    {
        return port;
    }

    @Config("csv.protocol")
    public CSVConfig setProtocol(String protocol)
    {
        this.protocol = protocol;
        return this;
    }

    @Config("csv.base")
    public CSVConfig setBase(String base)
    {
        this.base = checkPath(base);
        return this;
    }

    @Config("csv.table-description-dir")
    public CSVConfig setTableDescriptionDir(String tableDescriptionDir)
    {
        this.tableDescriptionDir = checkPath(tableDescriptionDir);
        return this;
    }

    @Config("csv.username")
    public CSVConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("csv.password")
    public CSVConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Config("csv.host")
    public CSVConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Config("csv.port")
    public CSVConfig setPort(Integer port)
    {
        this.port = port;
        return this;
    }

    private String checkPath(String path)
    {
        if (!path.startsWith("/")) {
            return "/" + path;
        }
        if (path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }
}
