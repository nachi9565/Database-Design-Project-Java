package tableDataStructureFiles;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.System.out;

import java.util.List;
import java.util.Map;

import baseFile.DavisBase;
import tableFiles.Column;
import tableFiles.Condition;
import tableFiles.DataType;
import tableFiles.Field;
import tableFiles.MetaData;

public class DavisBaseBinaryFile {

    public static String columnsTable = "DavisBase_columns";
    public static String tablesTable = "DavisBase_tables";
    public static boolean showRowId = false;
    public static boolean dataStoreInitialized = false;

    static int pageSizePower = 9;
    public static int pageSize = (int) Math.pow(2, pageSizePower);

    RandomAccessFile file;

    public DavisBaseBinaryFile(RandomAccessFile file) {
        this.file = file;
    }

    public boolean recordExists(MetaData tablemetaData, List<String> columNames, Condition condition) throws IOException {

        BPlusTree bPlusTree = new BPlusTree(file, tablemetaData.rootPageNo, tablemetaData.tableName);


        for (Integer pageNo : bPlusTree.getAllLeaves(condition)) {
            Page page = new Page(file, pageNo);
            for (TableRecord record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(record.getFields().get(condition.columnOrdinal).fieldValue))
                        continue;
                }
                return true;
            }
        }
        return false;

    }


    public int updateRecords(MetaData tablemetaData, Condition condition,
                             List<String> columNames, List<String> newValues) throws IOException {
        int count = 0;


        List<Integer> ordinalPostions = tablemetaData.getOrdinalPostions(columNames);

        int k = 0;
        Map<Integer, Field> newValueMap = new HashMap<>();

        for (String strnewValue : newValues) {
            int index = ordinalPostions.get(k);

            try {
                newValueMap.put(index,
                        new Field(tablemetaData.columnNameAttrs.get(index).dataType, strnewValue));
            } catch (Exception e) {
                System.out.println("Invalid data format for " + tablemetaData.columnNames.get(index) + " values: "
                        + strnewValue);
                return count;
            }

            k++;
        }

        BPlusTree bPlusTree = new BPlusTree(file, tablemetaData.rootPageNo, tablemetaData.tableName);

        for (Integer pageNo : bPlusTree.getAllLeaves(condition)) {
            short deleteCountPerPage = 0;
            Page page = new Page(file, pageNo);
            for (TableRecord record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(record.getFields().get(condition.columnOrdinal).fieldValue))
                        continue;
                }
                count++;
                for (int i : newValueMap.keySet()) {
                    Field oldValue = record.getFields().get(i);
                    int rowId = record.rowId;
                    if ((record.getFields().get(i).dataType == DataType.TEXT
                            && record.getFields().get(i).fieldValue.length() == newValueMap.get(i).fieldValue.length())
                            || (record.getFields().get(i).dataType != DataType.NULL && record.getFields().get(i).dataType != DataType.TEXT)
                    ) {
                        page.updateRecord(record, i, newValueMap.get(i).fieldValueByte);
                    } else {

                        page.DeleteTableRecord(tablemetaData.tableName,
                                Integer.valueOf(record.pageHeaderIndex - deleteCountPerPage).shortValue());
                        deleteCountPerPage++;
                        List<Field> attrs = record.getFields();
                        Field attr = attrs.get(i);
                        attrs.remove(i);
                        attr = newValueMap.get(i);
                        attrs.add(i, attr);
                        rowId = page.addTableRow(tablemetaData.tableName, attrs);
                    }

                    if (tablemetaData.columnNameAttrs.get(i).hasIndex && condition != null) {
                        RandomAccessFile indexFile = new RandomAccessFile(DavisBase.getNDXFilePath(tablemetaData.columnNameAttrs.get(i).tableName, tablemetaData.columnNameAttrs.get(i).columnName), "rw");
                        Tree bTree = new Tree(indexFile);
                        bTree.delete(oldValue, record.rowId);
                        bTree.insert(newValueMap.get(i), rowId);
                        indexFile.close();
                    }

                }
            }
        }

        if (!tablemetaData.tableName.equals(tablesTable) && !tablemetaData.tableName.equals(columnsTable))
            System.out.println("* " + count + " record(s) updated.");

        return count;

    }

    public void selectRecords(MetaData tablemetaData, List<String> columNames, Condition condition) throws IOException {

        List<Integer> ordinalPostions = tablemetaData.getOrdinalPostions(columNames);

        System.out.println();

        List<Integer> printPosition = new ArrayList<>();

        int columnPrintLength = 0;
        printPosition.add(columnPrintLength);
        int totalTablePrintLength = 0;
        if (showRowId) {
            System.out.print("rowid");
            System.out.print(DavisBase.line(" ", 5));
            printPosition.add(10);
            totalTablePrintLength += 10;
        }


        for (int i : ordinalPostions) {
            String columnName = tablemetaData.columnNameAttrs.get(i).columnName;
            columnPrintLength = Math.max(columnName.length()
                    , tablemetaData.columnNameAttrs.get(i).dataType.getPrintOffset()) + 5;
            printPosition.add(columnPrintLength);
            System.out.print(columnName);
            System.out.print(DavisBase.line(" ", columnPrintLength - columnName.length()));
            totalTablePrintLength += columnPrintLength;
        }
        System.out.println();
        System.out.println(DavisBase.line("-", totalTablePrintLength));

        BPlusTree bPlusTree = new BPlusTree(file, tablemetaData.rootPageNo, tablemetaData.tableName);

        String currentValue = "";
        for (Integer pageNo : bPlusTree.getAllLeaves(condition)) {
            Page page = new Page(file, pageNo);
            for (TableRecord record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(record.getFields().get(condition.columnOrdinal).fieldValue))
                        continue;
                }
                int columnCount = 0;
                if (showRowId) {
                    currentValue = Integer.valueOf(record.rowId).toString();
                    System.out.print(currentValue);
                    System.out.print(DavisBase.line(" ", printPosition.get(++columnCount) - currentValue.length()));
                }
                for (int i : ordinalPostions) {
                    currentValue = record.getFields().get(i).fieldValue;
                    System.out.print(currentValue);
                    System.out.print(DavisBase.line(" ", printPosition.get(++columnCount) - currentValue.length()));
                }
                System.out.println();
            }
        }

        System.out.println();

    }


    public static int getRootPageNo(RandomAccessFile binaryfile) {
        int rootpage = 0;
        try {
            for (int i = 0; i < binaryfile.length() / DavisBaseBinaryFile.pageSize; i++) {
                binaryfile.seek(i * DavisBaseBinaryFile.pageSize + 0x0A);
                int a = binaryfile.readInt();

                if (a == -1) {
                    return i;
                }
            }
            return rootpage;
        } catch (Exception e) {
            out.println("error while getting root page no ");
            out.println(e);
        }
        return -1;

    }

    public static void initializeDataStore() {

        try {
            File dataDir = new File("data");
            dataDir.mkdir();
            String[] oldTableFiles;
            oldTableFiles = dataDir.list();
            for (int i = 0; i < oldTableFiles.length; i++) {
                File anOldFile = new File(dataDir, oldTableFiles[i]);
                anOldFile.delete();
            }
        } catch (SecurityException se) {
            out.println("Unable to create data container directory");
            out.println(se);
        }

        try {

            int currentPageNo = 0;

            RandomAccessFile DavisBaseTablesCatalog = new RandomAccessFile(
                    DavisBase.getTBLFilePath(tablesTable), "rw");
            Page.addNewPage(DavisBaseTablesCatalog, PageType.LEAF, -1, -1);
            Page page = new Page(DavisBaseTablesCatalog, currentPageNo);

            page.addTableRow(tablesTable, Arrays.asList(new Field(DataType.TEXT, DavisBaseBinaryFile.tablesTable),
                    new Field(DataType.INT, "2"),
                    new Field(DataType.SMALLINT, "0"),
                    new Field(DataType.SMALLINT, "0")));

            page.addTableRow(tablesTable, Arrays.asList(new Field(DataType.TEXT, DavisBaseBinaryFile.columnsTable),
                    new Field(DataType.INT, "11"),
                    new Field(DataType.SMALLINT, "0"),
                    new Field(DataType.SMALLINT, "2")));

            DavisBaseTablesCatalog.close();
        } catch (Exception e) {
            out.println("Unable to create the database_tables file");
            out.println(e);


        }

        try {
            RandomAccessFile DavisBaseColumnsCatalog = new RandomAccessFile(
                    DavisBase.getTBLFilePath(columnsTable), "rw");
            Page.addNewPage(DavisBaseColumnsCatalog, PageType.LEAF, -1, -1);
            Page page = new Page(DavisBaseColumnsCatalog, 0);

            short ordinal_position = 1;

            page.addNewColumn(new Column(tablesTable, DataType.TEXT, "table_name", true, false, ordinal_position++));
            page.addNewColumn(new Column(tablesTable, DataType.INT, "record_count", false, false, ordinal_position++));
            page.addNewColumn(new Column(tablesTable, DataType.SMALLINT, "avg_length", false, false, ordinal_position++));
            page.addNewColumn(new Column(tablesTable, DataType.SMALLINT, "root_page", false, false, ordinal_position++));

            ordinal_position = 1;

            page.addNewColumn(new Column(columnsTable, DataType.TEXT, "table_name", false, false, ordinal_position++));
            page.addNewColumn(new Column(columnsTable, DataType.TEXT, "column_name", false, false, ordinal_position++));
            page.addNewColumn(new Column(columnsTable, DataType.SMALLINT, "data_type", false, false, ordinal_position++));
            page.addNewColumn(new Column(columnsTable, DataType.SMALLINT, "ordinal_position", false, false, ordinal_position++));
            page.addNewColumn(new Column(columnsTable, DataType.TEXT, "is_nullable", false, false, ordinal_position++));
            page.addNewColumn(new Column(columnsTable, DataType.SMALLINT, "column_key", false, true, ordinal_position++));
            page.addNewColumn(new Column(columnsTable, DataType.SMALLINT, "is_unique", false, false, ordinal_position++));

            DavisBaseColumnsCatalog.close();
            dataStoreInitialized = true;
        } catch (Exception e) {
            out.println("Unable to create the database_columns file");
            out.println(e);
        }
    }
}


