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
package org.apache.drill.exec.store.excel.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.ListUtils;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.List;
import java.util.Objects;

// В drill появился свой format plugin для excel. Поэтому просто переименовываем наш.
// На данный момент в idvp data он не используется.
@JsonTypeName("idvp-excel")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExcelFormatConfig implements FormatPluginConfig {

    public static ExcelFormatConfig DEFAULT = new ExcelFormatConfig();

    private final List<String> extensions;
    private final boolean extractHeaders;
    private final boolean evaluateFormula;

    private ExcelFormatConfig() {
        this(null, null, null);
    }

    @JsonCreator
    private ExcelFormatConfig(@JsonProperty("extensions") List<String> extensions,
                              @JsonProperty("extractHeaders") Boolean extractHeaders,
                              @JsonProperty("evaluateFormula") Boolean evaluateFormula) {
        this.extensions = ListUtils.defaultIfNull(extensions, ImmutableList.of("xls", "xlsx"));
        this.extractHeaders = extractHeaders == null || extractHeaders;
        this.evaluateFormula = evaluateFormula != null && evaluateFormula;
    }

    public List<String> getExtensions() {
        return extensions;
    }

    public Boolean isExtractHeaders() {
        return extractHeaders;
    }

    public boolean isEvaluateFormula() {
        return evaluateFormula;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExcelFormatConfig that = (ExcelFormatConfig) o;
        return extractHeaders == that.extractHeaders &&
                evaluateFormula == that.evaluateFormula &&
                Objects.equals(extensions, that.extensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extensions, extractHeaders, evaluateFormula);
    }
}