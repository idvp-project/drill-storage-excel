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

import com.monitorjbl.xlsx.StreamingReader;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Oleg Zinoviev
 * @since 15.01.18.
 */
public class CellRangeReaderTest {

    @Test
    public void test1ReadXlsx1() throws IOException, InvalidFormatException {
        CellRange range = new CellRange(1, 7, 0, 2);
        List<String[]> result = readRange("src/test/resources/test1.xlsx", range);
        Assert.assertEquals(7, result.size());
        assertNullArray(result, 2);
        assertNullArray(result, 4);
        assertNullArray(result, 5);
    }

    @Test
    public void test1ReadXlsx2() throws IOException, InvalidFormatException {
        CellRange range = new CellRange(3, 7, 0, 2);
        List<String[]> result = readRange("src/test/resources/test1.xlsx", range);
        Assert.assertEquals(5, result.size());
        assertNullArray(result, 0);
        assertNullArray(result, 2);
        assertNullArray(result, 3);
    }

    @Test
    public void test1ReadXlsx3() throws IOException, InvalidFormatException {
        CellRange range = new CellRange(5, 6, 0, 2);
        List<String[]> result = readRange("src/test/resources/test1.xlsx", range);
        Assert.assertEquals(2, result.size());
        assertNullArray(result, 0);
        assertNullArray(result, 1);
    }

    @Test
    public void test2ReadXlsx1() throws IOException, InvalidFormatException {
        CellRange range = new CellRange(1, 100, 0, 6);
        List<String[]> result = readRange("src/test/resources/test2.xlsx", range);
        Assert.assertEquals(100, result.size());
        assertNullArray(result, 2);
        for (int i = 5; i < 79; i++) {
            assertNullArray(result, i);
        }
        for (int i = 80; i < 100; i++) {
            assertNullArray(result, i);
        }

    }


    private List<String[]> readRange(String fileName, CellRange range) throws IOException, InvalidFormatException {
        Workbook sheets = StreamingReader.builder()
                .rowCacheSize(100)
                .bufferSize(4096)
                .open(new File(fileName));
        Sheet sheet = sheets.getSheetAt(0);

        List<String[]> result = new ArrayList<>();
        CellRangeReader reader = new CellRangeReader(sheet, range, false);
        while (reader.hasNext()) {
            result.add(reader.next());
        }

        return result;
    }

    private void assertNullArray(List<String[]> result, int index) {
        if (result.size() <= index) {
            throw new AssertionError("Index ouf of range");
        }

        String[] strings = result.get(index);
        for (String str : strings) {
            if (str != null) {
                throw new AssertionError("Not null value at " + index);
            }
        }
    }
}
