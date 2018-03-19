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
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.excel.read.ExcelRecordReader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created by mnasyrov on 11.08.2017.
 */
@SuppressWarnings("unused")
public class ExcelScanCreator implements BatchCreator<ExcelSubScan> {

    private CloseableRecordBatch createBatchScan(FragmentContext context, ExcelSubScan scan) throws
            IOException {

        ExcelStoragePlugin storagePlugin = scan.getStoragePlugin();
        OperatorContext operatorContext = context.newOperatorContext(scan);
        DrillFileSystem dfs = operatorContext.newFileSystem(storagePlugin.getFsConf());

        RuntimeExcelTableConfig runtimeConfig = scan.getStoragePluginConfig().getRuntimeConfig(scan.getSpec().getTable());

        RecordReader reader = new ExcelRecordReader(dfs, scan.getColumns(), runtimeConfig);

        return new ScanBatch(context, operatorContext, Collections.singletonList(reader), Collections.emptyList());
    }


    @Override
    public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
                                         ExcelSubScan scan,
                                         List<RecordBatch> children) throws ExecutionSetupException {
        assert children == null || children.isEmpty();
        try {
            return createBatchScan(context, scan);
        } catch (IOException e) {
            throw new ExecutionSetupException(e);
        }
    }
}