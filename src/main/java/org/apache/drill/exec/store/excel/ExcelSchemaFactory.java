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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StoragePlugin;

import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by mnasyrov on 11.08.2017.
 */
public class ExcelSchemaFactory implements SchemaFactory {

    private final String schemaName;
    private final ExcelStoragePlugin plugin;

    ExcelSchemaFactory(String schemaName, ExcelStoragePlugin plugin) {
        this.schemaName = schemaName;
        this.plugin = plugin;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
        ExcelSchema schema = new ExcelSchema(schemaName);
        parent.add(schemaName, schema);
    }

    class ExcelSchema extends AbstractSchema {

        private final ConcurrentMap<String, ExcelTable> tables = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);

        ExcelSchema(String name) {
            super(Collections.emptyList(), name);
        }

        @Override
        public String getTypeName() {
            return ExcelStoragePluginConfig.NAME;
        }

        @Override
        public Table getTable(String name) {
            return tables.computeIfAbsent(name, n -> new ExcelTable(plugin, schemaName, new ExcelScanSpec(n)));
        }
    }

    static class ExcelTable extends DynamicDrillTable {

        ExcelTable(StoragePlugin plugin, String storageEngineName, ExcelScanSpec selection) {
            super(plugin, storageEngineName, selection);
        }
    }
}
