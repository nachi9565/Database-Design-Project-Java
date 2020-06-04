package baseFile;



import tableFiles.Column;
import tableFiles.Condition;
import tableFiles.DataType;
import tableFiles.Field;
import tableFiles.Messages;
import tableFiles.MetaData;

import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import tableDataStructureFiles.BPlusTree;
import tableDataStructureFiles.DavisBaseBinaryFile;
import tableDataStructureFiles.Page;
import tableDataStructureFiles.PageType;
import tableDataStructureFiles.TableRecord;
import tableDataStructureFiles.Tree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.out;

public class DavisBase {

    static String prompt = "davisbase> ";
    static boolean isExit = false;
    static long pageSize = 512;

    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String[] args) {

        splashScreen();

        File dataDir = new File("data");

        if (!new File(dataDir, DavisBaseBinaryFile.tablesTable + ".tbl").exists()
                || !new File(dataDir, DavisBaseBinaryFile.columnsTable + ".tbl").exists())
            DavisBaseBinaryFile.initializeDataStore();
        else
            DavisBaseBinaryFile.dataStoreInitialized = true;

        String userCommand = "";

        while (!isExit) {
            System.out.print(prompt);
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
            parseUserCommand(userCommand);
        }
        System.out.println(Messages.EXIT);
    }

    public static void splashScreen() {
        System.out.println("Welcome to DavisBase (\"help;\"  - display all commands available)");
        System.out.println();
    }

    public static String line(String s, int num) {
        String a = "";
        for (int i = 0; i < num; i++) {
            a += s;
        }
        return a;
    }

    public static void printCmd(String s) {
        System.out.println("\n\t" + s + "\n");
    }

    public static void printDef(String s) {
        System.out.println("\t\t" + s);
    }

    public static void help() {
        out.println(line("-", 80));
        out.println("COMMANDS AVAILABLE:\n");

        out.println("1. For showing all tables available in the database:\t\tSHOW TABLES;");

        out.println("2. For create a new table with columns and values:\t\tCREATE TABLE <table_name> (<column_name> <data_type> <not_null> <unique> <primary key>);");

        out.println("3. For deleting a table:\t\t\t\t\tDROP TABLE <table_name>;");

        out.println("4. For inserting a new record into desired table:\t\tINSERT INTO <table_name> (<column_list>) VALUES (<values_list>);");

        out.println("5. For selecting records(with or without conditions):\t\tSELECT <column_list> FROM <table_name> [WHERE <condition>];");

        out.println("6. For help:\t\t\t\t\t\t\tHELP;");

        out.println("7. To exit:\t\t\t\t\t\t\tEXIT;");

        out.println(line("-", 80));
    }

    public static void parseUserCommand(String userCommand) {

        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
        switch (commandTokens.get(0)) {
            case "show":
                if (commandTokens.get(1).equals("tables"))
                    parseUserCommand("select * from DavisBase_tables");
                else if (commandTokens.get(1).equals("rowid")) {
                    DavisBaseBinaryFile.showRowId = true;
                    System.out.println(Messages.INCLUDE_ROW_ID);
                } else
                    System.out.println(Messages.DONT_UNDERSTAND + userCommand + "\"");
                break;
            case "select":
                parseQuery(userCommand);
                break;
            case "drop":
                dropTable(userCommand);
                break;
            case "create":
                if (commandTokens.get(1).equals("table"))
                    parseCreateTable(userCommand);
                else if (commandTokens.get(1).equals("index"))
                    parseCreateIndex(userCommand);
                break;
            case "insert":
                parseInsert(userCommand);
                break;
            case "delete":
                parseDelete(userCommand);
                break;
            case "help":
                help();
                break;
            case "exit":
                isExit = true;
                break;
            case "quit":
                isExit = true;
                break;
            case "test":
                test();
                break;
            default:
                System.out.println(Messages.DONT_UNDERSTAND + userCommand + "\"");
                break;
        }
    }

    public static void test() {
        Scanner scan = new Scanner(System.in);
        parseUserCommand("create table test (id int, name text)");
        scan.nextLine();
        parseUserCommand("create index on test (name)");
        scan.nextLine();
        for (int i = 1; i < 35; i++) {
            parseUserCommand("insert into test (id , name) values (" + (i) + ", 'uzu'" + i + " )");
        }
        parseUserCommand("show tables");

        scan.nextLine();

    }

    public static void parseCreateIndex(String createIndexString) {
        ArrayList<String> createIndexTokens = new ArrayList<String>(Arrays.asList(createIndexString.split(" ")));
        try {
            if (!createIndexTokens.get(2).equals("on") || !createIndexString.contains("(")
                    || !createIndexString.contains(")") && createIndexTokens.size() < 4) {
                System.out.println(Messages.SYNTAX_ERROR);
                return;
            }

            String tableName = createIndexString
                    .substring(createIndexString.indexOf("on") + 3, createIndexString.indexOf("(")).trim();
            String columnName = createIndexString
                    .substring(createIndexString.indexOf("(") + 1, createIndexString.indexOf(")")).trim();

            if (new File(DavisBase.getNDXFilePath(tableName, columnName)).exists()) {
                System.out.println(Messages.INDEX_EXISTS);
                return;
            }

            RandomAccessFile tableFile = new RandomAccessFile(getTBLFilePath(tableName), "rw");

            MetaData metaData = new MetaData(tableName);

            if (!metaData.tableExists) {
                System.out.println(Messages.INV_TABLE_NAME);
                tableFile.close();
                return;
            }

            int columnOrdinal = metaData.columnNames.indexOf(columnName);

            if (columnOrdinal < 0) {
                System.out.println(Messages.INV_COL_NAME);
                tableFile.close();
                return;
            }


            RandomAccessFile indexFile = new RandomAccessFile(getNDXFilePath(tableName, columnName), "rw");
            Page.addNewPage(indexFile, PageType.LEAFINDEX, -1, -1);


            if (metaData.recordCount > 0) {
                BPlusTree bPlusTree = new BPlusTree(tableFile, metaData.rootPageNo, metaData.tableName);
                for (int pageNo : bPlusTree.getAllLeaves()) {
                    Page page = new Page(tableFile, pageNo);
                    Tree bTree = new Tree(indexFile);
                    for (TableRecord record : page.getPageRecords()) {
                        bTree.insert(record.getFields().get(columnOrdinal), record.rowId);
                    }
                }
            }

            System.out.println(Messages.INDEX_CREATED + columnName);
            indexFile.close();
            tableFile.close();

        } catch (IOException e) {

            System.out.println(Messages.INDEX_ERROR);
            System.out.println(e);
        }

    }


    public static void dropTable(String dropTableString) {
        String[] tokens = dropTableString.split(" ");
        if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
            System.out.println("Error");
            return;
        }

        ArrayList<String> dropTableTokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
        String tableName = dropTableTokens.get(2);


        parseDelete("delete from table " + DavisBaseBinaryFile.tablesTable + " where table_name = '" + tableName + "' ");
        parseDelete("delete from table " + DavisBaseBinaryFile.columnsTable + " where table_name = '" + tableName + "' ");
        File tableFile = new File("data/" + tableName + ".tbl");
        if (tableFile.delete()) {
            System.out.println(Messages.TABLE_DELETED);
        } else System.out.println(Messages.TABLE_NOT_EXISTS);


        File f = new File("data/");
        File[] matchingFiles = f.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(tableName) && name.endsWith("ndx");
            }
        });
        boolean iFlag = false;
        for (File file : matchingFiles) {
            if (file.delete()) {
                iFlag = true;
                System.out.println(Messages.INDEX_DELETED);
            }
        }
        if (iFlag)
            System.out.println("drop " + tableName);
        else
            System.out.println(Messages.INDEX_NOT_EXISTS);
    }

    public static void parseQuery(String queryString) {
        String table_name = "";
        List<String> column_names = new ArrayList<String>();

        // Get table and column names for the select
        ArrayList<String> queryTableTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
        int i = 0;

        for (i = 1; i < queryTableTokens.size(); i++) {
            if (queryTableTokens.get(i).equals("from")) {
                ++i;
                table_name = queryTableTokens.get(i);
                break;
            }
            if (!queryTableTokens.get(i).equals("*") && !queryTableTokens.get(i).equals(",")) {
                if (queryTableTokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<String>(
                            Arrays.asList(queryTableTokens.get(i).split(",")));
                    for (String col : colList) {
                        column_names.add(col.trim());
                    }
                } else
                    column_names.add(queryTableTokens.get(i));
            }
        }

        MetaData tableMetaData = new MetaData(table_name);
        if (!tableMetaData.tableExists) {
            System.out.println(Messages.TABLE_NOT_EXISTS);
            return;
        }

        Condition condition = null;
        try {

            condition = extractConditionFromQuery(tableMetaData, queryString);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        if (column_names.size() == 0) {
            column_names = tableMetaData.columnNames;
        }
        try {

            RandomAccessFile tableFile = new RandomAccessFile(getTBLFilePath(table_name), "r");
            DavisBaseBinaryFile tableBinaryFile = new DavisBaseBinaryFile(tableFile);
            tableBinaryFile.selectRecords(tableMetaData, column_names, condition);
            tableFile.close();
        } catch (IOException exception) {
            System.out.println(Messages.ERROR_SEL_COL);
        }

    }

    public static void parseInsert(String queryString) {

        ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));

        if (!insertTokens.get(1).equals("into") || !queryString.contains(") values")) {
            System.out.println(Messages.SYNTAX_ERROR);
//            System.out.println("Expected Syntax: INSERT INTO table_name ( columns ) VALUES ( values );");
            return;
        }

        try {
            String tableName = insertTokens.get(2);
            if (tableName.trim().length() == 0) {
                System.out.println(Messages.TABLE_NAME_EMPTY);
                return;
            }

            if (tableName.indexOf("(") > -1) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }
            MetaData dstMetaData = new MetaData(tableName);

            if (!dstMetaData.tableExists) {
                System.out.println(Messages.TABLE_NOT_EXISTS);
                return;
            }

            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(
                    queryString.substring(queryString.indexOf("(") + 1, queryString.indexOf(") values")).split(",")));

            for (String colToken : columnTokens) {
                if (!dstMetaData.columnNames.contains(colToken.trim())) {
                    System.out.println(Messages.INV_COL_NAME + " : " + colToken.trim());
                    return;
                }
            }

            String valuesString = queryString.substring(queryString.indexOf("values") + 6, queryString.length() - 1);

            ArrayList<String> valueTokens = new ArrayList<String>(Arrays
                    .asList(valuesString.substring(valuesString.indexOf("(") + 1).split(",")));

            List<Field> fieldToInsert = new ArrayList<>();

            for (Column colInfo : dstMetaData.columnNameAttrs) {
                int i = 0;
                boolean columnProvided = false;
                for (i = 0; i < columnTokens.size(); i++) {
                    if (columnTokens.get(i).trim().equals(colInfo.columnName)) {
                        columnProvided = true;
                        try {
                            String value = valueTokens.get(i).replace("'", "").replace("\"", "").trim();
                            if (valueTokens.get(i).trim().equals("null")) {
                                if (!colInfo.isNullable) {
                                    System.out.println("Cannot Insert NULL into " + colInfo.columnName);
                                    return;
                                }
                                colInfo.dataType = DataType.NULL;
                                value = value.toUpperCase();
                            }
                            Field attr = new Field(colInfo.dataType, value);
                            fieldToInsert.add(attr);
                            break;
                        } catch (Exception e) {
                            System.out.println("Invalid data format for " + columnTokens.get(i) + " values: "
                                    + valueTokens.get(i));
                            return;
                        }
                    }
                }
                if (columnTokens.size() > i) {
                    columnTokens.remove(i);
                    valueTokens.remove(i);
                }

                if (!columnProvided) {
                    if (colInfo.isNullable)
                        fieldToInsert.add(new Field(DataType.NULL, "NULL"));
                    else {
                        System.out.println("Cannot Insert NULL into " + colInfo.columnName);
                        return;
                    }
                }
            }

            RandomAccessFile dstTable = new RandomAccessFile(getTBLFilePath(tableName), "rw");
            int dstPageNo = BPlusTree.getPageNoForInsert(dstTable, dstMetaData.rootPageNo);
            Page dstPage = new Page(dstTable, dstPageNo);

            int rowNo = dstPage.addTableRow(tableName, fieldToInsert);

            // update Index
            if (rowNo != -1) {

                for (int i = 0; i < dstMetaData.columnNameAttrs.size(); i++) {
                    Column col = dstMetaData.columnNameAttrs.get(i);

                    if (col.hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(getNDXFilePath(tableName, col.columnName),
                                "rw");
                        Tree bTree = new Tree(indexFile);
                        bTree.insert(fieldToInsert.get(i), rowNo);
                    }

                }
            }

            dstTable.close();
            if (rowNo != -1)
                System.out.println("Record Inserted");
            System.out.println();

        } catch (Exception ex) {
            System.out.println(Messages.ERROR_INSERTING);
            System.out.println(ex);

        }
    }


    public static void parseCreateTable(String createTableString) {

        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

        if (!createTableTokens.get(1).equals("table")) {
            System.out.println(Messages.SYNTAX_ERROR);
            return;
        }
        String tableName = createTableTokens.get(2);
        if (tableName.trim().length() == 0) {
            System.out.println(Messages.TABLE_NAME_EMPTY);
            return;
        }
        try {

            if (tableName.indexOf("(") > -1) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }

            List<Column> lstcolumnInformation = new ArrayList<>();
            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(createTableString
                    .substring(createTableString.indexOf("(") + 1, createTableString.length() - 1).split(",")));

            short ordinalPosition = 1;

            String primaryKeyColumn = "";

            for (String columnToken : columnTokens) {

                ArrayList<String> colInfoToken = new ArrayList<String>(Arrays.asList(columnToken.trim().split(" ")));
                Column colInfo = new Column();
                colInfo.tableName = tableName;
                colInfo.columnName = colInfoToken.get(0);
                colInfo.isNullable = true;
                colInfo.dataType = DataType.get(colInfoToken.get(1).toUpperCase());
                for (int i = 0; i < colInfoToken.size(); i++) {

                    if ((colInfoToken.get(i).equals("null"))) {
                        colInfo.isNullable = true;
                    }
                    if (colInfoToken.get(i).contains("not") && (colInfoToken.get(i + 1).contains("null"))) {
                        colInfo.isNullable = false;
                        i++;
                    }

                    if ((colInfoToken.get(i).equals("unique"))) {
                        colInfo.isUnique = true;
                    } else if (colInfoToken.get(i).contains("primary") && (colInfoToken.get(i + 1).contains("key"))) {
                        colInfo.isPrimaryKey = true;
                        colInfo.isUnique = true;
                        colInfo.isNullable = false;
                        primaryKeyColumn = colInfo.columnName;
                        i++;
                    }

                }
                colInfo.ordinalPosition = ordinalPosition++;
                lstcolumnInformation.add(colInfo);

            }

            RandomAccessFile DavisBaseTablesCatalog = new RandomAccessFile(
                    getTBLFilePath(DavisBaseBinaryFile.tablesTable), "rw");
            MetaData DavisBaseTableMetaData = new MetaData(DavisBaseBinaryFile.tablesTable);

            int pageNo = BPlusTree.getPageNoForInsert(DavisBaseTablesCatalog, DavisBaseTableMetaData.rootPageNo);

            Page page = new Page(DavisBaseTablesCatalog, pageNo);

            int rowNo = page.addTableRow(DavisBaseBinaryFile.tablesTable,
                    Arrays.asList(new Field(DataType.TEXT, tableName), // sdl.files.DavisBaseBinaryFile.tablesTable->test
                            new Field(DataType.INT, "0"), new Field(DataType.SMALLINT, "0"),
                            new Field(DataType.SMALLINT, "0")));
            DavisBaseTablesCatalog.close();

            if (rowNo == -1) {
                System.out.println(Messages.DUPLICATE_TABLE_NAME);
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(getTBLFilePath(tableName), "rw");
            Page.addNewPage(tableFile, PageType.LEAF, -1, -1);
            tableFile.close();

            RandomAccessFile DavisBaseColumnsCatalog = new RandomAccessFile(
                    getTBLFilePath(DavisBaseBinaryFile.columnsTable), "rw");
            MetaData DavisBaseColumnsMetaData = new MetaData(DavisBaseBinaryFile.columnsTable);
            pageNo = BPlusTree.getPageNoForInsert(DavisBaseColumnsCatalog, DavisBaseColumnsMetaData.rootPageNo);

            Page page1 = new Page(DavisBaseColumnsCatalog, pageNo);

            for (Column column : lstcolumnInformation) {
                page1.addNewColumn(column);
            }

            DavisBaseColumnsCatalog.close();

            System.out.println("Table created");

            if (primaryKeyColumn.length() > 0) {
                parseCreateIndex("create index on " + tableName + "(" + primaryKeyColumn + ")");
            }
        } catch (Exception e) {

            System.out.println(Messages.ERROR_CREATING_TABLE);
            System.out.println(e.getMessage());
            parseDelete("delete from table " + DavisBaseBinaryFile.tablesTable + " where table_name = '" + tableName
                    + "' ");
            parseDelete("delete from table " + DavisBaseBinaryFile.columnsTable + " where table_name = '" + tableName
                    + "' ");
        }

    }

    private static void parseDelete(String deleteTableString) {
        ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(deleteTableString.split(" ")));

        String tableName = "";

        try {

            if (!deleteTableTokens.get(1).equals("from") || !deleteTableTokens.get(2).equals("table")) {
                System.out.println(Messages.SYNTAX_ERROR);
                return;
            }

            tableName = deleteTableTokens.get(3);

            MetaData metaData = new MetaData(tableName);
            Condition condition = null;
            try {
                condition = extractConditionFromQuery(metaData, deleteTableString);

            } catch (Exception e) {
                System.out.println(e);
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(getTBLFilePath(tableName), "rw");

            BPlusTree tree = new BPlusTree(tableFile, metaData.rootPageNo, metaData.tableName);
            List<TableRecord> deletedRecords = new ArrayList<TableRecord>();
            int count = 0;
            for (int pageNo : tree.getAllLeaves(condition)) {
                short deleteCountPerPage = 0;
                Page page = new Page(tableFile, pageNo);
                for (TableRecord record : page.getPageRecords()) {
                    if (condition != null) {
                        if (!condition.checkCondition(record.getFields().get(condition.columnOrdinal).fieldValue))
                            continue;
                    }

                    deletedRecords.add(record);
                    page.DeleteTableRecord(tableName,
                            Integer.valueOf(record.pageHeaderIndex - deleteCountPerPage).shortValue());
                    deleteCountPerPage++;
                    count++;
                }
            }


            if (condition == null) {
                // delete index files
            } else {
                for (int i = 0; i < metaData.columnNameAttrs.size(); i++) {
                    if (metaData.columnNameAttrs.get(i).hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(getNDXFilePath(tableName, metaData.columnNameAttrs.get(i).columnName), "rw");
                        Tree bTree = new Tree(indexFile);
                        for (TableRecord record : deletedRecords) {
                            bTree.delete(record.getFields().get(i), record.rowId);
                        }
                    }
                }
            }

            System.out.println();
            tableFile.close();
            System.out.println(count + " record(s) deleted!");

        } catch (Exception e) {
            System.out.println("Error on deleting rows in table : " + tableName);
            System.out.println(e.getMessage());
        }

    }

    public static String getTBLFilePath(String tableName) {
        return "data/" + tableName + ".tbl";
    }

    public static String getNDXFilePath(String tableName, String columnName) {
        return "data/" + tableName + "_" + columnName + ".ndx";
    }

    private static Condition extractConditionFromQuery(MetaData tableMetaData, String query) throws Exception {
        if (query.contains("where")) {
            Condition condition = new Condition(DataType.TEXT);
            String whereClause = query.substring(query.indexOf("where") + 6);
            ArrayList<String> whereClauseTokens = new ArrayList<String>(Arrays.asList(whereClause.split(" ")));

            if (whereClauseTokens.get(0).equalsIgnoreCase("not")) {
                condition.setNegation(true);
            }


            for (int i = 0; i < Condition.supportedOperators.length; i++) {
                if (whereClause.contains(Condition.supportedOperators[i])) {
                    whereClauseTokens = new ArrayList<String>(
                            Arrays.asList(whereClause.split(Condition.supportedOperators[i])));
                    {
                        condition.setOperator(Condition.supportedOperators[i]);
                        condition.setConditionValue(whereClauseTokens.get(1).trim());
                        condition.setColumName(whereClauseTokens.get(0).trim());
                        break;
                    }

                }
            }


            if (tableMetaData.tableExists
                    && tableMetaData.columnExists(new ArrayList<String>(Arrays.asList(condition.columnName)))) {
                condition.columnOrdinal = tableMetaData.columnNames.indexOf(condition.columnName);
                condition.dataType = tableMetaData.columnNameAttrs.get(condition.columnOrdinal).dataType;
            } else {
                throw new Exception(
                        "Invalid Table/Column : " + tableMetaData.tableName + " . " + condition.columnName);
            }
            return condition;
        } else
            return null;
    }

}