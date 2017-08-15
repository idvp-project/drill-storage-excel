# drill-storage-excel
Apache Drill Storage Plugin for reading Excel files

**Usage:**
```json
{
  "type": "excel",
  "enabled": true,
  "connection": "file:///C:/data/4drill/lol",
  "config": null,
  "tables": {
    "orders": {
      "location": "222.xlsx",
      "worksheet": "Sheet1",
      "cellRange": "M12:V14",
      "floatingRangeFooter": true,
      "extractHeaders": true
    }
  }
}
```
Apache Drill Format Plugin for reading Excel files

**Usage:**
```json
{
  "type": "file",
  "enabled": true,
  "connection": "file:///",
  "config": null,
  "workspaces": {
    "root": {
      "location": "/",
      "writable": false,
      "defaultInputFormat": null
    }
  },
  "formats": {    
    "excel": {
      "type": "excel",
      "extensions": [
        "xlsx",
        "xls"
      ],
      "extractHeaders": true
    }
  }
}
```