package tableFiles;

import java.io.File;

import baseFile.DavisBase;

/* Class to denote column name and datatype  of table metadata */
public class Column {
    public DataType dataType;

    public String columnName;

    public boolean isUnique;
    public boolean isNullable;
    public Short ordinalPosition;
    public boolean hasIndex;
    public String tableName;
    public boolean isPrimaryKey;

    public Column() {

    }

    public Column(String tableName, DataType dataType, String columnName, boolean isUnique, boolean isNullable, short ordinalPosition) {
        this.dataType = dataType;
        this.columnName = columnName;
        this.isUnique = isUnique;
        this.isNullable = isNullable;
        this.ordinalPosition = ordinalPosition;
        this.tableName = tableName;

        this.hasIndex = (new File(DavisBase.getNDXFilePath(tableName, columnName)).exists());

    }


    public void setAsPrimaryKey() {
        isPrimaryKey = true;
    }
}