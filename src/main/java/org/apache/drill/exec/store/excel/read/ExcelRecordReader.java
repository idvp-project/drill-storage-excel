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

import io.netty.buffer.DrillBuf;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.excel.RuntimeExcelTableConfig;
import org.apache.drill.exec.util.DecimalUtility;
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
    private static final int MAX_RECORDS_PER_BATCH = 8096;
    private static final String DEFAULT_COLUMN_NAME = "column";

    private final FragmentContext fragmentContext;
    private final DrillFileSystem fileSystem;
    private final List<SchemaPath> requestedColumns;
    private final RuntimeExcelTableConfig config;
    private final boolean unionEnabled;
    private final boolean stringify;

    private DrillBuf buffer;
    private VectorContainerWriter writer;
    private Workbook wb;
    private FSDataInputStream fsStream;
    private CellRangeReader cellRangeReader;

    private Map<Integer, String> headers;

    public ExcelRecordReader(FragmentContext fragmentContext, DrillFileSystem fileSystem, List<SchemaPath> columns, RuntimeExcelTableConfig config) {
        assert config != null : "RuntimeExcelTableConfig must be passed";
        this.fragmentContext = fragmentContext;
        this.fileSystem = fileSystem;
        this.requestedColumns = columns;
        this.config = config;
        this.unionEnabled = fragmentContext.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
        this.stringify = config.isStringify();
    }

    public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
        try {
            this.writer = new VectorContainerWriter(output, unionEnabled);
            this.buffer = fragmentContext.getManagedBuffer();
            this.fsStream = fileSystem.open(config.getLocation());

            this.wb = WorkbookFactory.create(fsStream);
            final Sheet sheet = config.getWorksheet() == null ? wb.getSheetAt(0) : wb.getSheet(config.getWorksheet());

            CellRange cellRange = new CellRangeBuilder()
                    .withRange(this.config.getCellRange())
                    .withFloatingFooter(this.config.isFloatingRangeFooter())
                    .withSheet(sheet)
                    .build();

            this.cellRangeReader = new CellRangeReader(sheet, cellRange, config.isStringify());
            this.headers = this.config.isExtractHeaders() ? extractHeaders(cellRangeReader.next()) : new HashMap<>();

            setColumns(this.requestedColumns);

            if (!isStarQuery()) {
                Set<Integer> columnFilter = new HashSet<>();
                for (SchemaPath path : requestedColumns) {
                    String col = path.getRootSegment().getPath();
                    Integer key = headers.entrySet()
                            .stream()
                            .filter(c -> col.equals(c.getValue()))
                            .map(Map.Entry::getKey)
                            .findFirst()
                            .orElse(null);
                    if (key != null) {
                        columnFilter.add(key);
                    }
                }

                cellRangeReader.setColumnFilter(columnFilter);
            }

        } catch (IOException | InvalidFormatException | CellRangeReaderException e) {
            logger.debug("ExcelRecordReader: " + e.getMessage());
            throw new ExecutionSetupException(e);
        }
    }

    private Map<Integer, String> extractHeaders(Map<Integer, Object> row) {
        Map<Integer, String> result = new HashMap<>();
        for (Map.Entry<Integer, Object> entry : row.entrySet()) {
            result.put(entry.getKey(), createColumnName(Objects.toString(entry.getValue(), null), entry.getKey()));
        }

        return result;
    }

    private Map<String, Integer> columnNameCounters = new HashMap<>();

    private String createColumnName(String name, Integer colNum) {
        if (StringUtils.isBlank(name)) {
            name = DEFAULT_COLUMN_NAME;
        }

        name = name.replace('.', '_');

        if (!columnNameCounters.containsKey(name)) {
            columnNameCounters.put(name, 0);
        }

        Integer nameCounter = columnNameCounters.get(name);

        String result;
        if (DEFAULT_COLUMN_NAME.equals(name))
            result = String.format("%s%s", name, colNum);
        else
            result = String.format("%s%s", name, nameCounter.equals(0) ? "" : nameCounter);

        columnNameCounters.put(name, ++nameCounter);
        return result;
    }


    public int next() {
        this.writer.allocate();
        this.writer.reset();

        int recordCount = 0;

        try {
            BaseWriter.MapWriter map = this.writer.rootAsMap();

            while (cellRangeReader.hasNext() && recordCount < MAX_RECORDS_PER_BATCH) {
                Map<Integer, Object> next = cellRangeReader.next();
                if (next
                        .values()
                        .stream()
                        .anyMatch(Objects::nonNull)
                    || stringify) {

                    this.writer.setPosition(recordCount);
                    map.start();

                    for (Map.Entry<Integer, Object> entry : next.entrySet()) {
                        if (!headers.containsKey(entry.getKey())) {
                            headers.put(entry.getKey(), createColumnName(DEFAULT_COLUMN_NAME, entry.getKey()));
                        }
                        map(map, headers.get(entry.getKey()), entry.getValue());
                    }

                    map.end();
                    recordCount++;
                }
            }

            this.writer.setValueCount(recordCount);
            return recordCount;
        } catch (final Exception e) {
            throw UserException.dataReadError(e).build(logger);
        }
    }

    private void map(BaseWriter.MapWriter map, String key, Object value) {
        if (value == null) {
            if (stringify) {
                map.varChar(key);
            }
        }


        if (value instanceof String) {
            byte[] bytes = String.class.cast(value).getBytes(StandardCharsets.UTF_8);
            this.buffer = buffer.reallocIfNeeded(bytes.length);
            this.buffer.setBytes(0, bytes);
            map.varChar(key).writeVarChar(0, bytes.length, buffer);
        }

        if (value instanceof Boolean) {
            Boolean bit = Boolean.class.cast(value);
            map.bit(key).writeBit(bit ? 1 : 0);
        }

        if (value instanceof Double) {
            Decimal18Holder h = new Decimal18Holder();
            BigDecimal d = new BigDecimal(Double.class.cast(value));
            h.precision = d.precision();
            h.scale = d.scale();
            h.value = DecimalUtility.getDecimal18FromBigDecimal(d, d.scale(), d.precision());
            map.decimal18(key, d.scale(), d.precision()).write(h);
        }

        if (value instanceof Date) {
            TimeStampHolder h = new TimeStampHolder();
            h.value = ((Date) value).getTime();
            map.timeStamp(key).write(h);
        }
    }

    public void close() throws Exception {
        this.wb.close();
        this.fsStream.close();
    }
}
