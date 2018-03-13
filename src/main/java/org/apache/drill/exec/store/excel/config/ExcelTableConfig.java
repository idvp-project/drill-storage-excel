/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.excel.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Created by mnasyrov on 11.08.2017.
 */
public class ExcelTableConfig {
    private final String location;
    private final String worksheet;
    private final String cellRange;
    private final boolean floatingRangeFooter;
    private final boolean extractHeaders;
    private final boolean evaluateFormula;

    public ExcelTableConfig(@JsonProperty("location") String location,
                            @JsonProperty("worksheet") String worksheet,
                            @JsonProperty("cellRange") String cellRange,
                            @JsonProperty("floatingRangeFooter") Boolean floatingRangeFooter,
                            @JsonProperty("extractHeaders") Boolean extractHeaders,
                            @JsonProperty("evaluateFormula") Boolean evaluateFormula) {
        this.location = location;
        this.worksheet = worksheet;
        this.cellRange = cellRange;
        this.floatingRangeFooter = floatingRangeFooter == null ? true : floatingRangeFooter;
        this.extractHeaders = extractHeaders == null ? true : extractHeaders;
        this.evaluateFormula = evaluateFormula == null ? false : evaluateFormula;
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

    public boolean isFloatingRangeFooter() {
        return floatingRangeFooter;
    }

    public boolean isExtractHeaders() {
        return extractHeaders;
    }

    public boolean isEvaluateFormula() {
        return evaluateFormula;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExcelTableConfig that = (ExcelTableConfig) o;
        return floatingRangeFooter == that.floatingRangeFooter &&
                extractHeaders == that.extractHeaders &&
                evaluateFormula == that.evaluateFormula &&
                Objects.equals(location, that.location) &&
                Objects.equals(worksheet, that.worksheet) &&
                Objects.equals(cellRange, that.cellRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, worksheet, cellRange, floatingRangeFooter, extractHeaders, evaluateFormula);
    }
}
