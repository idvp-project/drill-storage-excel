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

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.poi.ss.usermodel.*;

import java.util.Iterator;
import java.util.Optional;

/**
 * Created by mnasyrov on 14.08.2017.
 */
public class CellRangeReader implements Iterator<String[]> {
    private final CellRange cellRange;
    private final FormulaEvaluator evaluator;
    private final DataFormatter dataFormatter;
    private final Iterator<Row> rowIterator;

    private int index;
    private final int lastRow;
    private Row lastRiddenRow;

    CellRangeReader(Sheet sheet,
                    CellRange cellRange,
                    boolean evaluateFormula) {
        this.cellRange = cellRange;
        this.evaluator = evaluateFormula ? sheet.getWorkbook().getCreationHelper().createFormulaEvaluator() : null;
        this.dataFormatter = new DataFormatter();

        this.index = cellRange.getRowStart();
        this.lastRow = cellRange.getRowEnd();

        this.rowIterator = sheet.rowIterator();
        
    }

    @Override
    public boolean hasNext() {
        return index <= lastRow;
    }

    @Override
    public String[] next() {
        if (!hasNext()) {
            throw new CellRangeReaderException("Invalid read operation");
        }

        String[] result = new String[Math.abs(cellRange.getColEnd() - cellRange.getColStart()) + 1];
        try {
            if (lastRiddenRow != null && index < lastRiddenRow.getRowNum()) {
                //getRowNum - 0-based. Поэтому добавляем к getRowNum 1
                //Последняя прочитанная строка находится ЗА индексом
                //Возвращаем пустоту
                return result;
            }


            if (lastRiddenRow == null || index > lastRiddenRow.getRowNum()) {
                if (rowIterator.hasNext()) {
                    while (rowIterator.hasNext()) {
                        lastRiddenRow = rowIterator.next();
                        if (lastRiddenRow.getRowNum() >= cellRange.getRowStart()) {
                            break;
                        }
                    }
                } else {
                    //Если нет следующей строки -
                    lastRiddenRow = null;
                }
            }

            if (lastRiddenRow == null || index < lastRiddenRow.getRowNum()) {
                return result;
            }

            int startCell = this.cellRange.getColStart();
            int lastCell = this.cellRange.getColEnd();

            if (startCell >= 0 && lastCell >= 0 && lastCell >= startCell) {
                for (int cn = startCell; cn <= lastCell; cn++) {
                    if (lastRiddenRow == null) {
                        result[cn - startCell] = null;
                    } else {
                        Cell cell = lastRiddenRow.getCell(cn);
                        result[cn - startCell] = Optional
                                .ofNullable(cell)
                                .map(this::getCellValue)
                                .orElse(null);
                    }
                }
            }
        } finally {
            index++;
        }

        return result;
    }

    private String getCellValue(Cell cell) {
        try {
            return dataFormatter.formatCellValue(cell, evaluator);
        } catch (Exception e) {
            String message = String.format("Cannot read value in cell %s[%d, %d]", cell.getSheet().getSheetName(), cell.getRowIndex(), cell.getColumnIndex());
            throw new DrillRuntimeException(message, e);
        }
    }
}
