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

import org.apache.drill.exec.store.excel.config.ExcelFormatConfig;
import org.apache.drill.exec.store.excel.config.ExcelTableConfig;
import org.apache.hadoop.fs.Path;

/**
 * Created by mnasyrov on 11.08.2017.
 */
public class RuntimeExcelTableConfig {

    private final Path location;
    private final String worksheet;
    private final String cellRange;
    private final boolean floatingRangeFooter;
    private final boolean extractHeaders;

    RuntimeExcelTableConfig(ExcelStoragePluginConfig storagePluginConfig, ExcelTableConfig tableConfig) {
        this.location = new Path(storagePluginConfig.connection, tableConfig.getLocation());
        this.worksheet = tableConfig.getWorksheet();
        this.cellRange = tableConfig.getCellRange();
        this.floatingRangeFooter = tableConfig.isFloatingRangeFooter();
        this.extractHeaders = tableConfig.isExtractHeaders();
    }

    RuntimeExcelTableConfig(String filePath, ExcelFormatConfig formatConfig) {
        this.location = new Path(filePath);
        this.extractHeaders = formatConfig.isExtractHeaders();
        this.worksheet = null;
        this.cellRange = null;
        this.floatingRangeFooter = false;
    }

    public Path getLocation() {
        return location;
    }

    public String getWorksheet() {
        return worksheet;
    }

    public String getCellRange() {
        return cellRange;
    }

    public boolean isExtractHeaders() {
        return extractHeaders;
    }

    public boolean isFloatingRangeFooter() {
        return floatingRangeFooter;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RuntimeExcelTableConfig that = (RuntimeExcelTableConfig) o;

        if (floatingRangeFooter != that.floatingRangeFooter) return false;
        if (extractHeaders != that.extractHeaders) return false;
        if (location != null ? !location.equals(that.location) : that.location != null) return false;
        if (worksheet != null ? !worksheet.equals(that.worksheet) : that.worksheet != null) return false;
        return cellRange != null ? cellRange.equals(that.cellRange) : that.cellRange == null;
    }

    @Override
    public int hashCode() {
        int result = location != null ? location.hashCode() : 0;
        result = 31 * result + (worksheet != null ? worksheet.hashCode() : 0);
        result = 31 * result + (cellRange != null ? cellRange.hashCode() : 0);
        result = 31 * result + (floatingRangeFooter ? 1 : 0);
        result = 31 * result + (extractHeaders ? 1 : 0);
        return result;
    }
}
