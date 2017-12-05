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
package org.apache.drill.exec.store.excel.read;

import org.apache.poi.ss.usermodel.*;

import java.util.Iterator;
import java.util.Optional;

/**
 * Created by mnasyrov on 14.08.2017.
 */
public class CellRangeReader implements Iterator<String[]> {
    private final Sheet sheet;
    private final CellRange cellRange;
    private final FormulaEvaluator evaluator;
    private final DataFormatter dataFormatter;

    private int index;
    private final int lastRow;

    CellRangeReader(Sheet sheet,
                    CellRange cellRange) {
        this.sheet = sheet;
        this.cellRange = cellRange;
        this.evaluator = sheet.getWorkbook().getCreationHelper().createFormulaEvaluator();
        this.dataFormatter = new DataFormatter();

        this.index = cellRange.getRowStart();
        this.lastRow = cellRange.getRowEnd();
    }

    @Override
    public boolean hasNext() {
        return index <= lastRow;
    }

    @Override
    public String[] next() {
        if(!hasNext()) {
            throw new CellRangeReaderException("Invalid read operation");
        }

        String[] result = new String[Math.abs(cellRange.getColEnd() - cellRange.getColStart()) + 1];
        Row row = sheet.getRow(index);
        index++;


        int startCell = this.cellRange.getColStart();
        int lastCell = this.cellRange.getColEnd();

        if(startCell >= 0 && lastCell >= 0 && lastCell >= startCell) {
            for (int cn = startCell; cn <= lastCell; cn++) {
                if (row == null) {
                    result[cn - startCell] = null;
                } else {
                    Cell cell = row.getCell(cn);
                    result[cn - startCell] = Optional
                            .ofNullable(cell)
                            .map(this::getCellValue)
                            .orElse(null);
                }
            }
        }
        return result;
    }

    private String getCellValue(Cell cell) {
        return dataFormatter.formatCellValue(cell, evaluator);
    }
}
