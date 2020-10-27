/*
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

import com.fasterxml.jackson.annotation.*;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.io.IOException;
import java.util.List;

/**
 * Created by mnasyrov on 11.08.2017.
 */
@JsonTypeName("excel-sub-scan")
public class ExcelSubScan extends AbstractSubScan {

    private final ExcelScanSpec spec;
    private final List<SchemaPath> columns;
    private final ExcelStoragePlugin storagePlugin;
    private final ExcelStoragePluginConfig storagePluginConfig;

    @JsonCreator
    private ExcelSubScan(@JsonProperty("userName") final String userName,
                         @JsonProperty("spec") final ExcelScanSpec excelScanSpec,
                         @JsonProperty("columns") final List<SchemaPath> columns,
                         @JsonProperty("storagePluginConfig") final ExcelStoragePluginConfig storagePluginConfig,
                         @JacksonInject final StoragePluginRegistry pluginRegistry) {
        this(userName, excelScanSpec, pluginRegistry.resolve(storagePluginConfig, ExcelStoragePlugin.class), columns);
    }

    ExcelSubScan(final String userName,
                 final ExcelScanSpec spec,
                 final ExcelStoragePlugin storagePlugin,
                 final List<SchemaPath> columns) {
        super(userName);
        this.spec = spec;
        this.columns = columns;
        this.storagePlugin = storagePlugin;
        this.storagePluginConfig = storagePlugin.getConfig();
        initialAllocation = 100;
    }

    @JsonProperty
    public ExcelScanSpec getSpec() {
        return spec;
    }

    @JsonProperty
    public ExcelStoragePluginConfig getStoragePluginConfig() {
        return storagePluginConfig;
    }

    @Override
    public int getOperatorType() {
        return UserBitShared.CoreOperatorType.DIRECT_SUB_SCAN_VALUE;
    }

    @JsonProperty("columns")
    public List<SchemaPath> getColumns() {
        return this.columns;
    }

    @JsonIgnore
    ExcelStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    @Override
    public PhysicalOperator getNewWithChildren(final List<PhysicalOperator> children) throws ExecutionSetupException {
        return super.getNewWithChildren(children);
    }

}
