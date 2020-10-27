/*
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

import com.github.pjfanning.xlsx.StreamingReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.text.reader.HeaderBuilder;
import org.apache.drill.exec.store.excel.RuntimeExcelTableConfig;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Created by mnasyrov on 10.08.2017.
 */
public class ExcelRecordReader extends AbstractRecordReader {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelRecordReader.class);
    private static final int MAX_RECORDS_PER_BATCH = 4096;

    private final DrillFileSystem fileSystem;
    private final RuntimeExcelTableConfig config;
    private Workbook wb;
    private CellRangeReader cellRangeReader;

    private List<NullableVarCharVector> vectors;
    private File tempFile;

    public ExcelRecordReader(DrillFileSystem fileSystem,
                             List<SchemaPath> columns,
                             RuntimeExcelTableConfig config) {
        assert config != null : "RuntimeExcelTableConfig must be passed";
        this.fileSystem = fileSystem;
        this.config = config;
        setColumns(columns);
    }

    public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
        try {
            this.tempFile = createTempFile();
            this.wb = readWorkbook(this.tempFile, config.isEvaluateFormula());
            final Sheet sheet = config.getWorksheet() == null ? wb.getSheetAt(0) : wb.getSheet(config.getWorksheet());

            CellRange cellRange = new CellRangeBuilder()
                    .withRange(this.config.getCellRange())
                    .withFloatingFooter(this.config.isFloatingRangeFooter())
                    .withSheet(sheet)
                    .build();

            this.cellRangeReader = new CellRangeReader(sheet, cellRange, config.isEvaluateFormula());

            final String[] headers;
            if (this.config.isExtractHeaders()) {
                headers = prepareHeaders(tempFile, cellRangeReader.next());
            } else {
                headers = prepareHeaders(tempFile, new String[Math.abs(cellRange.getColEnd() - cellRange.getColStart()) + 1]);
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

    private String[] prepareHeaders(File file,
                                    String[] headers) {
        if (headers == null || headers.length == 0) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }

        HeaderBuilder headerBuilder = new HeaderBuilder(new Path(file.toURI()));
        headerBuilder.startRecord();

        for (int i = 0; i < headers.length; i++) {
            headerBuilder.startField(i);
            for (byte b : StringUtils.defaultString(headers[i]).getBytes(StandardCharsets.UTF_8)) {
                headerBuilder.append(b);
            }
            headerBuilder.endField();
        }

        headerBuilder.finishRecord();
        return headerBuilder.getHeaders();
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
                vv.getMutator().setValueCount(Math.max(counter, 0));
            }

            return counter;
        } catch (final Exception e) {
            throw UserException.dataReadError(e).build(logger);
        }
    }

    public void close() throws Exception {
        this.wb.close();

        try {
            if (tempFile != null && tempFile.exists()) {
                // Пытаемся удалить файл
                //noinspection ResultOfMethodCallIgnored
                tempFile.delete();
            }
        } catch (Exception e) {
            logger.warn("Error deleting a temp file: " + tempFile, e);
        }

        if (this.config.isCloseFS()) {
            this.fileSystem.close();
        }
    }

    private File createTempFile() throws IOException {
        File tempFile = File.createTempFile("excel", ".tmp");
        try (InputStream is = fileSystem.open(config.getLocation())) {
            try (FileOutputStream os = new FileOutputStream(tempFile)) {
                IOUtils.copy(is, os);
            }
        }
        return tempFile;
    }

    private Workbook readWorkbook(File file,
                                  boolean evaluateFormula) throws IOException, InvalidFormatException {
        if (!file.exists()) {
            throw new FileNotFoundException(file.toString());
        }

        try (InputStream inp = new FileInputStream(file)) {
            InputStream is = FileMagic.prepareToCheckMagic(inp);
            FileMagic fm = FileMagic.valueOf(is);
            switch (fm) {
                case OLE2:
                    POIFSFileSystem fs = new POIFSFileSystem(is);
                    return WorkbookFactory.create(fs);
                case OOXML:
                    if (evaluateFormula) {
                        return new XSSFWorkbook(OPCPackage.open(is));
                    } else {
                        return StreamingReader.builder()
                                .rowCacheSize(100)
                                .bufferSize(4096)
                                .open(file);
                    }
                default:
                    throw new InvalidFormatException("Your InputStream was neither an OLE2 stream, nor an OOXML stream");
            }
        }
    }
}
