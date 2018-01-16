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

import com.monitorjbl.xlsx.StreamingReader;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author Oleg Zinoviev
 * @since 16.01.18.
 */
public class SheetRangeDetectorTest {

    @Test
    public void test1() throws IOException, InvalidFormatException {
        CellRange range = readRange("src/test/resources/test1.xlsx");
        Assert.assertNotNull(range);
        Assert.assertEquals(0, range.getRowStart());
        Assert.assertEquals(6, range.getRowEnd());
        Assert.assertEquals(0, range.getColStart());
        Assert.assertEquals(2, range.getColEnd());

        CellRangeAddress address = CellRangeAddress.valueOf("A1:C7");
        Assert.assertEquals(address.getFirstRow(), range.getRowStart());
        Assert.assertEquals(address.getLastRow(), range.getRowEnd());
        Assert.assertEquals(address.getFirstColumn(), range.getColStart());
        Assert.assertEquals(address.getLastColumn(), range.getColEnd());
    }

    @Test
    public void test2() throws IOException, InvalidFormatException {
        CellRange range = readRange("src/test/resources/test2.xlsx");
        Assert.assertNotNull(range);
        Assert.assertEquals(0, range.getRowStart());
        Assert.assertEquals(79, range.getRowEnd());
        Assert.assertEquals(0, range.getColStart());
        Assert.assertEquals(6, range.getColEnd());

        CellRangeAddress address = CellRangeAddress.valueOf("A1:G80");
        Assert.assertEquals(address.getFirstRow(), range.getRowStart());
        Assert.assertEquals(address.getLastRow(), range.getRowEnd());
        Assert.assertEquals(address.getFirstColumn(), range.getColStart());
        Assert.assertEquals(address.getLastColumn(), range.getColEnd());

    }

    private CellRange readRange(String fileName) throws IOException, InvalidFormatException {
        Workbook sheets = StreamingReader.builder()
                .rowCacheSize(100)
                .bufferSize(4096)
                .open(new File(fileName));
        Sheet sheet = sheets.getSheetAt(0);
        return new SheetRangeDetector(sheet).detectRange();
    }


}
