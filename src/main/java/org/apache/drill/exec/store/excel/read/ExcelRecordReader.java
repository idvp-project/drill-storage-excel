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
package org.apache.drill.exec.store.excel.read;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.DrillBuf;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.excel.RuntimeExcelTableConfig;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by mnasyrov on 10.08.2017.
 */
public class ExcelRecordReader extends AbstractRecordReader {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelRecordReader.class);
    private static final int MAX_RECORDS_PER_BATCH = 4096;
    private static final String DEFAULT_COLUMN_NAME = "column";

    private final DrillFileSystem fileSystem;
    private final RuntimeExcelTableConfig config;
    private Workbook wb;
    private FSDataInputStream fsStream;
    private CellRangeReader cellRangeReader;
    private final Map<String, Integer> columnNameCounters = new HashMap<>();

    private List<NullableVarCharVector> vectors;

    public ExcelRecordReader(DrillFileSystem fileSystem,
                             RuntimeExcelTableConfig config) {
        assert config != null : "RuntimeExcelTableConfig must be passed";
        this.fileSystem = fileSystem;
        this.config = config;
    }

    public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
        try {
            this.fsStream = fileSystem.open(config.getLocation());

            this.wb = WorkbookFactory.create(fsStream);
            final Sheet sheet = config.getWorksheet() == null ? wb.getSheetAt(0) : wb.getSheet(config.getWorksheet());

            CellRange cellRange = new CellRangeBuilder()
                    .withRange(this.config.getCellRange())
                    .withFloatingFooter(this.config.isFloatingRangeFooter())
                    .withSheet(sheet)
                    .build();

            this.cellRangeReader = new CellRangeReader(sheet, cellRange);

            final String[] headers;
            if (this.config.isExtractHeaders()) {
                headers = extractHeaders(cellRangeReader.next());
            } else {
                headers = extractHeaders(new String[Math.abs(cellRange.getColEnd() - cellRange.getColStart()) + 1]);
            }

            ImmutableList.Builder<NullableVarCharVector> vectorBuilder = ImmutableList.builder();
            for (String column : headers) {
                TypeProtos.MajorType type = Types.optional(TypeProtos.MinorType.VARCHAR);
                MaterializedField field = MaterializedField.create(column, type);
                NullableVarCharVector vector = output.addField(field, NullableVarCharVector.class);
                vectorBuilder.add(vector);
            }
            this.vectors = vectorBuilder.build();

        } catch (IOException
                | InvalidFormatException
                | CellRangeReaderException
                | SchemaChangeException e) {
            logger.error("ExcelRecordReader: " + e.getMessage());
            throw new ExecutionSetupException(e);
        }
    }

    private String[] extractHeaders(String[] row) {
        String[] result = ArrayUtils.clone(row);

        for (int i = 0; i < result.length; i++) {
            result[i] = createColumnName(result[i], i + 1);
        }

        return result;
    }

    private String createColumnName(String name, int column) {

        name = CharMatcher.JAVA_ISO_CONTROL
                .replaceFrom(StringUtils.defaultString(name), ' ');

        if (StringUtils.isBlank(name)) {
            name = DEFAULT_COLUMN_NAME;
        }

        if (!columnNameCounters.containsKey(name)) {
            columnNameCounters.put(name, 0);
        }

        Integer nameCounter = columnNameCounters.getOrDefault(name, 0);

        String result;
        if (DEFAULT_COLUMN_NAME.equals(name))
            result = String.format("%s%s", name, column);
        else
            result = String.format("%s%s", name, nameCounter.equals(0) ? "" : nameCounter);

        columnNameCounters.put(name, ++nameCounter);
        return result;
    }


    public int next() {
        int counter = 0;

        try {
            while (cellRangeReader.hasNext() && counter < MAX_RECORDS_PER_BATCH) {
                String[] row = cellRangeReader.next();

                for (int i = 0; i < row.length; i++) {
                    String value = row[i];
                    if (value == null) {
                        continue;
                    }
                    NullableVarCharVector valueVector = vectors.get(i);
                    byte[] record = value.getBytes(Charsets.UTF_8);
                    valueVector.getMutator().setSafe(counter, record, 0, record.length);
                }

                counter++;
            }

            for (ValueVector vv : vectors) {
                vv.getMutator().setValueCount(counter > 0 ? counter : 0);
            }

            return counter;
        } catch (final Exception e) {
            throw UserException.dataReadError(e).build(logger);
        }
    }

    public void close() throws Exception {
        this.wb.close();
        this.fsStream.close();
        if (this.config.isCloseFS()) {
            this.fileSystem.close();
        }
    }
}
