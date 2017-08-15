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

/**
 * Created by mnasyrov on 14.08.2017.
 */
public class CellRange {

    public Integer getRowStart() {
        return rowStart;
    }

    public Integer getRowEnd() {
        return rowEnd;
    }

    public Integer getColStart() {
        return colStart;
    }

    public Integer getColEnd() {
        return colEnd;
    }

    private final Integer rowStart;
    private final Integer rowEnd;
    private final Integer colStart;
    private final Integer colEnd;

    CellRange(Integer rowStart, Integer rowEnd, Integer colStart, Integer colEnd) {
        this.rowStart = rowStart;
        this.rowEnd = rowEnd;
        this.colStart = colStart;
        this.colEnd = colEnd;
    }

    boolean isColumnInRange(Integer colIndex) {
        return (colStart == null || colStart <= colIndex) && (colEnd == null || colEnd >= colIndex);
    }

    boolean isRowInRange(Integer rowIndex) {
        return (rowStart == null || rowStart <= rowIndex) && (rowEnd == null || rowEnd >= rowIndex);
    }
}
