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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.excel.config.iDVPExcelFormatConfig;
import org.apache.drill.exec.store.excel.read.ExcelRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

@SuppressWarnings({"WeakerAccess", "unused"})
public class iDVPExcelFormatPlugin extends EasyFormatPlugin<iDVPExcelFormatConfig> {

    private static final boolean IS_COMPRESSIBLE = false;
    private static final String DEFAULT_NAME = "excel";
    private final iDVPExcelFormatConfig config;

    public iDVPExcelFormatPlugin(DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
        this(DEFAULT_NAME, context, fsConf, storageConfig);
    }

    public iDVPExcelFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
        this(name, context, fsConf, storageConfig, iDVPExcelFormatConfig.DEFAULT);
    }

    public iDVPExcelFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config, iDVPExcelFormatConfig formatPluginConfig) {
        super(name, context, fsConf, config, formatPluginConfig, true, false, false, IS_COMPRESSIBLE, formatPluginConfig.getExtensions(), DEFAULT_NAME);
        this.config = formatPluginConfig;
    }

    @Override
    public boolean supportsPushDown() {
        return true;
    }

    @Override
    public RecordReader getRecordReader(FragmentContext fragmentContext, DrillFileSystem drillFileSystem, FileWork fileWork, List<SchemaPath> list, String s) throws ExecutionSetupException {
        return new ExcelRecordReader(drillFileSystem, list, new RuntimeExcelTableConfig(fileWork.getPath(), this.config));
    }

    @Override
    public RecordWriter getRecordWriter(FragmentContext fragmentContext, EasyWriter easyWriter) throws IOException {
        return null;
    }

    @Override
    public int getReaderOperatorType() {
        return UserBitShared.CoreOperatorType.JSON_SUB_SCAN_VALUE;
    }

    @Override
    public int getWriterOperatorType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsStatistics() {
        return false;
    }

    @Override
    public DrillStatsTable.TableStatistics readStatistics(FileSystem fileSystem, Path path) throws IOException {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public void writeStatistics(DrillStatsTable.TableStatistics tableStatistics, FileSystem fileSystem, Path path) throws IOException {
        throw new UnsupportedOperationException("unimplemented");
    }
}
