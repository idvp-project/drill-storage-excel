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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.util.Iterator;

/**
 * @author Oleg Zinoviev
 * @since 16.01.18.
 */
class SheetRangeDetector {
    private final Sheet sheet;

    SheetRangeDetector(Sheet sheet) {
        assert sheet != null : "sheet cannot be null";

        this.sheet = sheet;
    }

    CellRange detectRange() {
        int startRow = Integer.MAX_VALUE;
        int endRow = Integer.MIN_VALUE;

        int startColl = Integer.MAX_VALUE;
        int endColl = Integer.MIN_VALUE;

        Iterator<Row> rowIterator = sheet.rowIterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            if (row != null) {
                //rowNum - 0-based: https://poi.apache.org/apidocs/org/apache/poi/ss/usermodel/Sheet.html
                startRow = Math.min(startRow, row.getRowNum());
                endRow = Math.max(endRow, row.getRowNum());
                if (row.getPhysicalNumberOfCells() > 0) {
                    startColl = Math.min(startColl, row.getFirstCellNum());
                    //lastCellNum - index of last coll + 1: https://poi.apache.org/apidocs/org/apache/poi/ss/usermodel/Row.html
                    endColl = Math.max(endColl, row.getLastCellNum() - 1);
                }
            }
        }

        if (startColl == Integer.MAX_VALUE) {
            startColl = 0;
        }

        if (endColl == Integer.MIN_VALUE) {
            endColl = 0;
            startColl = 0;
        }

        if (startRow == Integer.MAX_VALUE) {
            startRow = 0;
        }

        if (endRow == Integer.MIN_VALUE) {
            endRow = 0;
            startRow = 0;
        }

        return new CellRange(startRow, endRow, startColl, endColl);
    }

}
