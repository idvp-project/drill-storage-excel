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

import org.apache.commons.lang.NotImplementedException;
import org.apache.poi.ss.usermodel.*;

import java.util.*;

/**
 * Created by mnasyrov on 14.08.2017.
 */
public class CellRangeReader implements Iterator<Map<Integer, Object>> {
    private final Sheet sheet;
    private final CellRange cellRange;
    private final FormulaEvaluator evaluator;
    private final boolean stringify;
    private final DataFormatter dataFormatter;

    private Set<Integer> filter = null;

    private int index;
    private final int lastRow;

    CellRangeReader(Sheet sheet, CellRange cellRange, boolean stringify) {
        this.sheet = sheet;
        this.cellRange = cellRange;
        this.evaluator = sheet.getWorkbook().getCreationHelper().createFormulaEvaluator();
        this.dataFormatter = new DataFormatter();

        this.index = cellRange.getRowStart() != null ? cellRange.getRowStart() : sheet.getFirstRowNum();
        this.lastRow = cellRange.getRowEnd() != null ? cellRange.getRowEnd() : sheet.getLastRowNum();
        this.stringify = stringify;
    }

    void setColumnFilter(Set<Integer> filter) {
        if(filter != null && cellRange != null) {
            for (Integer f : filter) {
                if(!cellRange.isColumnInRange(f)) {
                    throw new CellRangeReaderException("Columns filter is out of range bounds");
                }
            }
        }
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        return index <= lastRow;
    }

    @Override
    public Map<Integer, Object> next() {
        if(!hasNext()) {
            throw new CellRangeReaderException("Invalid read operation");
        }

        Map<Integer, Object> result = new LinkedHashMap<>();
        Row row = sheet.getRow(index);
        index++;

        if(this.filter != null) {
            for (Integer cn : this.filter) {
                Cell cell = row.getCell(cn);
                result.put(cn, cell != null ? getCellValue(cell) : null);
            }
            return result;
        }

        int startCell = this.cellRange != null ? this.cellRange.getColStart() != null ? this.cellRange.getColStart() : row.getFirstCellNum() : row.getFirstCellNum();
        int lastCell = this.cellRange != null ? this.cellRange.getColEnd() != null ? this.cellRange.getColEnd() : row.getLastCellNum() : row.getLastCellNum();

        if(startCell >= 0 && lastCell >= 0) {
            for (int cn = startCell; cn <= lastCell; cn++) {
                Cell cell = row.getCell(cn);
                result.put(cn, cell != null ? getCellValue(cell) : null);
            }
        }
        return result;
    }

    private Object getCellValue(Cell cell) {
        if (stringify) {
            return dataFormatter.formatCellValue(cell, evaluator);
        } else {

            switch (cell.getCellTypeEnum()) {
                case STRING:
                    return cell.getStringCellValue();
                case BOOLEAN:
                    return cell.getBooleanCellValue();
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        return cell.getDateCellValue();
                    }
                    return cell.getNumericCellValue();
                case FORMULA:
                    return getCellValue(evaluator.evaluate(cell));
                default:
                    return null;
            }
        }
    }

    private Object getCellValue(CellValue cell) {
        if (cell == null) {
            return null;
        }

        switch (cell.getCellTypeEnum()) {
            case STRING:
                return cell.getStringValue();
            case BOOLEAN:
                return cell.getBooleanValue();
            case NUMERIC:
                return cell.getNumberValue();
            default:
                return null;
        }
    }

    public void remove() {
        throw new NotImplementedException();
    }
}
