package tableFiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import baseFile.DavisBase;
import tableDataStructureFiles.BPlusTree;
import tableDataStructureFiles.DavisBaseBinaryFile;
import tableDataStructureFiles.Page;
import tableDataStructureFiles.TableRecord;

import java.io.IOException;
import java.io.RandomAccessFile;

public class MetaData {

    public int recordCount;
    public List<TableRecord> columnData;
    public List<Column> columnNameAttrs;
    public List<String> columnNames;
    public String tableName;
    public boolean tableExists;
    public int rootPageNo;
    public int lastRowId;

    public MetaData(String tableName) {
        this.tableName = tableName;
        tableExists = false;
        try {

            RandomAccessFile DavisBaseTablesCatalog = new RandomAccessFile(
                    DavisBase.getTBLFilePath(DavisBaseBinaryFile.tablesTable), "r");

            int rootPageNo = DavisBaseBinaryFile.getRootPageNo(DavisBaseTablesCatalog);

            BPlusTree bplusTree = new BPlusTree(DavisBaseTablesCatalog, rootPageNo, tableName);

            for (Integer pageNo : bplusTree.getAllLeaves()) {
                Page page = new Page(DavisBaseTablesCatalog, pageNo);

                for (TableRecord record : page.getPageRecords()) {
                    if (record.getFields().get(0).fieldValue.equals(tableName)) {
                        this.rootPageNo = Integer.parseInt(record.getFields().get(3).fieldValue);
                        recordCount = Integer.parseInt(record.getFields().get(1).fieldValue);
                        tableExists = true;
                        break;
                    }
                }
                if (tableExists)
                    break;
            }

            DavisBaseTablesCatalog.close();
            if (tableExists) {
                loadColumnData();
            } else {
                throw new Exception("Table does not exist.");
            }

        } catch (Exception e) {
        }
    }

    public List<Integer> getOrdinalPostions(List<String> columns) {
        List<Integer> ordinalPostions = new ArrayList<>();
        for (String column : columns) {
            ordinalPostions.add(columnNames.indexOf(column));
        }
        return ordinalPostions;
    }

    private void loadColumnData() {
        try {

            RandomAccessFile DavisBaseColumnsCatalog = new RandomAccessFile(
                    DavisBase.getTBLFilePath(DavisBaseBinaryFile.columnsTable), "r");
            int rootPageNo = DavisBaseBinaryFile.getRootPageNo(DavisBaseColumnsCatalog);

            columnData = new ArrayList<>();
            columnNameAttrs = new ArrayList<>();
            columnNames = new ArrayList<>();
            BPlusTree bPlusTree = new BPlusTree(DavisBaseColumnsCatalog, rootPageNo, tableName);

            for (Integer pageNo : bPlusTree.getAllLeaves()) {

                Page page = new Page(DavisBaseColumnsCatalog, pageNo);

                for (TableRecord record : page.getPageRecords()) {

                    if (record.getFields().get(0).fieldValue.equals(tableName)) {
                        {
                            columnData.add(record);
                            columnNames.add(record.getFields().get(1).fieldValue);
                            Column colInfo = new Column(
                                    tableName
                                    , DataType.get(record.getFields().get(2).fieldValue)
                                    , record.getFields().get(1).fieldValue
                                    , record.getFields().get(6).fieldValue.equals("YES")
                                    , record.getFields().get(4).fieldValue.equals("YES")
                                    , Short.parseShort(record.getFields().get(3).fieldValue)
                            );

                            if (record.getFields().get(5).fieldValue.equals("PRI"))
                                colInfo.setAsPrimaryKey();

                            columnNameAttrs.add(colInfo);


                        }
                    }
                }
            }

            DavisBaseColumnsCatalog.close();
        } catch (Exception e) {
            System.out.println("Error while getting column data for " + tableName);
        }

    }

    public boolean columnExists(List<String> columns) {

        if (columns.size() == 0)
            return true;

        List<String> lColumns = new ArrayList<>(columns);

        for (Column column_name_attr : columnNameAttrs) {
            lColumns.remove(column_name_attr.columnName);
        }

        return lColumns.isEmpty();
    }


    public void updateMetaData() {

        try {
            RandomAccessFile tableFile = new RandomAccessFile(
                    DavisBase.getTBLFilePath(tableName), "r");

            Integer rootPageNo = DavisBaseBinaryFile.getRootPageNo(tableFile);
            tableFile.close();


            RandomAccessFile DavisBaseTablesCatalog = new RandomAccessFile(
                    DavisBase.getTBLFilePath(DavisBaseBinaryFile.tablesTable), "rw");

            DavisBaseBinaryFile tablesBinaryFile = new DavisBaseBinaryFile(DavisBaseTablesCatalog);

            MetaData tablesMetaData = new MetaData(DavisBaseBinaryFile.tablesTable);

            Condition condition = new Condition(DataType.TEXT);
            condition.setColumName("table_name");
            condition.columnOrdinal = 0;
            condition.setConditionValue(tableName);
            condition.setOperator("=");

            List<String> columns = Arrays.asList("record_count", "root_page");
            List<String> newValues = new ArrayList<>();

            newValues.add(Integer.toString(recordCount));
            newValues.add(Integer.toString(rootPageNo));

            tablesBinaryFile.updateRecords(tablesMetaData, condition, columns, newValues);

            DavisBaseTablesCatalog.close();
        } catch (IOException e) {
            System.out.println("Error updating meta data for " + tableName);
        }


    }

    public boolean validateInsert(List<Field> row) throws IOException {
        RandomAccessFile tableFile = new RandomAccessFile(DavisBase.getTBLFilePath(tableName), "r");
        DavisBaseBinaryFile file = new DavisBaseBinaryFile(tableFile);


        for (int i = 0; i < columnNameAttrs.size(); i++) {

            Condition condition = new Condition(columnNameAttrs.get(i).dataType);
            condition.columnName = columnNameAttrs.get(i).columnName;
            condition.columnOrdinal = i;
            condition.setOperator("=");

            if (columnNameAttrs.get(i).isUnique) {
                condition.setConditionValue(row.get(i).fieldValue);
                if (file.recordExists(this, Arrays.asList(columnNameAttrs.get(i).columnName), condition)) {
                    System.out.println("Insert failed: Column " + columnNameAttrs.get(i).columnName + " should be unique.");
                    tableFile.close();
                    return false;
                }


            }


        }
        tableFile.close();
        return true;
    }

}