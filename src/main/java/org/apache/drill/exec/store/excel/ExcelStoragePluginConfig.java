/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.drill.exec.store.excel;

import com.fasterxml.jackson.annotation.*;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.exec.store.excel.config.ExcelTableConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Created by mnasyrov on 11.08.2017.
 */
@SuppressWarnings("WeakerAccess")
@JsonTypeName(ExcelStoragePluginConfig.NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExcelStoragePluginConfig extends StoragePluginConfigBase {

    static final String NAME = "excel";

    @JsonProperty("connection")
    private final String connection;

    @JsonProperty("config")
    private final Map<String, String> config;

    @JsonProperty("tables")
    private final Map<String, ExcelTableConfig> tables;

    @JsonCreator
    private ExcelStoragePluginConfig(@JsonProperty("connection") String connection,
                                     @JsonProperty("config") Map<String, String> config,
                                     @JsonProperty("tables") Map<String, ExcelTableConfig> tables) {
        this.connection = connection;
        this.config = config;
        this.tables = tables;
    }

    public String getConnection() {
        return connection;
    }

    public Map<String, String> getConfig() {
        return config == null ? Collections.emptyMap() : Collections.unmodifiableMap(config);
    }

    @SuppressWarnings("unused")
    public Map<String, ExcelTableConfig> getTables() {
        return tables == null ? Collections.emptyMap() : Collections.unmodifiableMap(tables);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExcelStoragePluginConfig that = (ExcelStoragePluginConfig) o;
        return Objects.equals(connection, that.connection) &&
                Objects.equals(config, that.config) &&
                Objects.equals(tables, that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connection, config, tables);
    }

    @JsonIgnore
    RuntimeExcelTableConfig getRuntimeConfig(String table) {
        ExcelTableConfig excelTableConfig = tables.get(table);

        if (excelTableConfig == null) {
            //Пробуем найти с ignoreCase:
            for (String key : tables.keySet()) {
                if (key != null && key.equalsIgnoreCase(table)) {
                    excelTableConfig = tables.get(key);
                    break;
                }
            }
        }

        if (excelTableConfig == null) {
            throw new IllegalArgumentException("Unknown table name" + table);
        }

        return new RuntimeExcelTableConfig(this, excelTableConfig);
    }

}
