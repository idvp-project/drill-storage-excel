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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.List;
import java.util.Objects;

@JsonTypeName("excel")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExcelFormatConfig implements FormatPluginConfig {

    public static ExcelFormatConfig DEFAULT = new ExcelFormatConfig();

    private List<String> extensions = ImmutableList.of("xls", "xlsx");
    private boolean extractHeaders;

    private ExcelFormatConfig () {}

    @JsonCreator
    public ExcelFormatConfig(@JsonProperty("extensions") List<String> extensions,
                             @JsonProperty("extractHeaders") Boolean extractHeaders) {
        this.extensions = extensions;
        this.extractHeaders = extractHeaders != null ? extractHeaders : true;
    }

    public List<String> getExtensions() {
        return extensions;
    }

    public Boolean isExtractHeaders() {
        return extractHeaders;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExcelFormatConfig that = (ExcelFormatConfig) o;
        return extractHeaders == that.extractHeaders &&
                Objects.equals(extensions, that.extensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extensions, extractHeaders);
    }
}