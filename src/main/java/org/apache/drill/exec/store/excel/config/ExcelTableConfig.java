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
package org.apache.drill.exec.store.excel.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by mnasyrov on 11.08.2017.
 */
public class ExcelTableConfig {
    private final String location;
    private final String worksheet;
    private final String cellRange;
    private final boolean floatingRangeFooter;
    private final boolean extractHeaders;

    public ExcelTableConfig(@JsonProperty("location") String location,
                           @JsonProperty("worksheet") String worksheet,
                           @JsonProperty("cellRange") String cellRange,
                           @JsonProperty("floatingRangeFooter") Boolean floatingRangeFooter,
                           @JsonProperty("extractHeaders") Boolean extractHeaders) {
        this.location = location;
        this.worksheet = worksheet;
        this.cellRange = cellRange;
        this.floatingRangeFooter = floatingRangeFooter != null ? floatingRangeFooter : true;
        this.extractHeaders = extractHeaders != null ? extractHeaders : true;
    }

    public String getLocation() {
        return location;
    }

    public String getWorksheet() {
        return worksheet;
    }

    public String getCellRange() {
        return cellRange;
    }

    public Boolean isFloatingRangeFooter() {
        return floatingRangeFooter;
    }


    public Boolean isExtractHeaders() {
        return extractHeaders;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExcelTableConfig that = (ExcelTableConfig) o;

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
