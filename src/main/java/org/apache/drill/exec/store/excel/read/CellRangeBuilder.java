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

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;

/**
 * Created by mnasyrov on 14.08.2017.
 */
class CellRangeBuilder {

    private String range;
    private Boolean floatingFooter;
    private Sheet sheet;

    CellRangeBuilder withRange(String range) {
        this.range = range;
        return this;
    }

    CellRangeBuilder withFloatingFooter(boolean floatingFooter) {
        this.floatingFooter = floatingFooter;
        return this;
    }

    CellRangeBuilder withSheet(Sheet sheet) {
        this.sheet = sheet;
        return this;
    }

    CellRange build() {
        if (sheet == null) {
            throw new IllegalStateException("Sheet not set");
        }

        if (range != null) {
            CellRangeAddress cr = CellRangeAddress.valueOf(range);
            int lastRow = cr.getLastRow();
            if (floatingFooter) {
                lastRow = new SheetRangeDetector(sheet).detectRange().getRowEnd();
            }

            return new CellRange(cr.getFirstRow(),
                    lastRow,
                    cr.getFirstColumn(),
                    cr.getLastColumn());
        } else {
            return new SheetRangeDetector(sheet).detectRange();
        }
    }
}
