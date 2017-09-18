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

/**
 * Created by mnasyrov on 14.08.2017.
 */
class CellRange {

    private final int rowStart;
    private final int rowEnd;
    private final int colStart;
    private final int colEnd;

    CellRange(int rowStart, int rowEnd, int colStart, int colEnd) {
        this.rowStart = rowStart;
        this.rowEnd = rowEnd;
        this.colStart = colStart;
        this.colEnd = colEnd;
    }

    int getRowStart() {
        return rowStart;
    }

    int getRowEnd() {
        return rowEnd;
    }

    int getColStart() {
        return colStart;
    }

    int getColEnd() {
        return colEnd;
    }

    boolean isColumnInRange(int colIndex) {
        return colStart <= colIndex && colEnd >= colIndex;
    }

}
