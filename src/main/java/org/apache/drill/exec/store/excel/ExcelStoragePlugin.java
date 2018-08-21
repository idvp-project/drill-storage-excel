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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.ClassPathFileSystem;
import org.apache.drill.exec.store.LocalSyncableFileSystem;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by mnasyrov on 10.08.2017.
 */
@SuppressWarnings("unused")
public class ExcelStoragePlugin extends AbstractStoragePlugin {

    private final ExcelSchemaFactory schemaFactory;
    private final ExcelStoragePluginConfig config;
    private final Configuration fsConf;

    public ExcelStoragePlugin(ExcelStoragePluginConfig config, DrillbitContext context, String name) {
        super(context, name);

        this.config = config;

        fsConf = new Configuration();
        for (Map.Entry<String, String> s : config.getConfig().entrySet()) {
            fsConf.set(s.getKey(), s.getValue());
        }

        fsConf.set(FileSystem.FS_DEFAULT_NAME_KEY, config.getConnection());
        fsConf.set("fs.classpath.impl", ClassPathFileSystem.class.getName());
        fsConf.set("fs.drill-local.impl", LocalSyncableFileSystem.class.getName());
        this.schemaFactory = new ExcelSchemaFactory(name, this);
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public ExcelStoragePluginConfig getConfig() {
        return config;
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
        ExcelScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<ExcelScanSpec>() {
        });
        return new ExcelGroupScan(userName, this, scanSpec, columns);
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
        schemaFactory.registerSchemas(schemaConfig, parent);
    }

    Configuration getFsConf() {
        return fsConf;
    }
}