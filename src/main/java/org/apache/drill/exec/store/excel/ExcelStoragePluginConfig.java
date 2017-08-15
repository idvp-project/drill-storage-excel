/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.excel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.exec.store.excel.config.ExcelTableConfig;

import java.util.Map;

/**
 * Created by mnasyrov on 11.08.2017.
 */
@JsonTypeName(ExcelStoragePluginConfig.NAME)
public class ExcelStoragePluginConfig extends StoragePluginConfigBase {

    static final String NAME = "excel";
    public String connection;
    public Map<String, String> config;
    public Map<String, ExcelTableConfig> tables;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExcelStoragePluginConfig that = (ExcelStoragePluginConfig) o;

        if (connection != null ? !connection.equals(that.connection) : that.connection != null) return false;
        if (config != null ? !config.equals(that.config) : that.config != null) return false;
        return tables != null ? tables.equals(that.tables) : that.tables == null;
    }

    @Override
    public int hashCode() {
        int result = connection != null ? connection.hashCode() : 0;
        result = 31 * result + (config != null ? config.hashCode() : 0);
        result = 31 * result + (tables != null ? tables.hashCode() : 0);
        return result;
    }


    @JsonIgnore
    RuntimeExcelTableConfig getRuntimeConfig(String table) {
        ExcelTableConfig excelTableConfig = tables.get(table);
        if(excelTableConfig == null) {
            throw new IllegalArgumentException("Unconfigured table " + table);
        }
        return new RuntimeExcelTableConfig(this, excelTableConfig);
    }

}
