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

import org.apache.poi.ss.util.CellRangeAddress;

/**
 * Created by mnasyrov on 14.08.2017.
 */
public class CellRangeBuilder {

    private String range;
    private Boolean floatingFooter;

    public CellRangeBuilder withRange(String range) {
        this.range = range;
        return this;
    }

    public CellRangeBuilder withFloatingFooter(boolean floatingFooter) {
        this.floatingFooter = floatingFooter;
        return this;
    }

    public CellRange build() {
        if(range != null) {
            CellRangeAddress cr = CellRangeAddress.valueOf(range);
            return new CellRange(cr.getFirstRow(), floatingFooter ? null : cr.getLastRow(), cr.getFirstColumn(), cr.getLastColumn());
        } else {
            return new CellRange(null, null, null, null);
        }
    }

}
