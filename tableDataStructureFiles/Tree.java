package tableDataStructureFiles;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tableFiles.Condition;
import tableFiles.DataType;
import tableFiles.Field;
import tableFiles.OperatorType;

public class Tree {
    Page root;
    RandomAccessFile binaryFile;

    public Tree(RandomAccessFile file) {
        this.binaryFile = file;
        this.root = new Page(binaryFile, DavisBaseBinaryFile.getRootPageNo(binaryFile));
    }

    private int getClosestPageNo(Page page, String value) {
        if (page.pageType == PageType.LEAFINDEX) {
            return page.pageNo;
        } else {
            if (Condition.compare(value, page.getIndexValues().get(0), page.indexValueDataType) < 0)
                return getClosestPageNo
                        (new Page(binaryFile, page.indexValuePointer.get(page.getIndexValues().get(0)).leftPageNo),
                                value);
            else if (Condition.compare(value, page.getIndexValues().get(page.getIndexValues().size() - 1), page.indexValueDataType) > 0)
                return getClosestPageNo(
                        new Page(binaryFile, page.rightPage),
                        value);
            else {
                String closestValue = binarySearch(page.getIndexValues().toArray(new String[page.getIndexValues().size()]), value, 0, page.getIndexValues().size() - 1, page.indexValueDataType);
                int i = page.getIndexValues().indexOf(closestValue);
                List<String> indexValues = page.getIndexValues();
                if (closestValue.compareTo(value) < 0 && i + 1 < indexValues.size()) {
                    return page.indexValuePointer.get(indexValues.get(i + 1)).leftPageNo;
                } else if (closestValue.compareTo(value) > 0) {
                    return page.indexValuePointer.get(closestValue).leftPageNo;
                } else {
                    return page.pageNo;
                }
            }
        }
    }


    public List<Integer> getRowIds(Condition condition) {
        List<Integer> rowIds = new ArrayList<>();

        Page page = new Page(binaryFile, getClosestPageNo(root, condition.comparisonValue));

        String[] indexValues = page.getIndexValues().toArray(new String[page.getIndexValues().size()]);

        OperatorType operationType = condition.getOperation();

        for (int i = 0; i < indexValues.length; i++) {
            if (condition.checkCondition(page.indexValuePointer.get(indexValues[i]).getIndexNode().indexValue.fieldValue))
                rowIds.addAll(page.indexValuePointer.get(indexValues[i]).rowIds);
        }

        if (operationType == OperatorType.LESSTHAN || operationType == OperatorType.LESSTHANOREQUAL) {
            if (page.pageType == PageType.LEAFINDEX)
                rowIds.addAll(getAllRowIdsLeftOf(page.parentPageNo, indexValues[0]));
            else
                rowIds.addAll(getAllRowIdsLeftOf(page.pageNo, condition.comparisonValue));
        }

        if (operationType == OperatorType.GREATERTHAN || operationType == OperatorType.GREATERTHANOREQUAL) {
            if (page.pageType == PageType.LEAFINDEX)
                rowIds.addAll(getAllRowIdsRightOf(page.parentPageNo, indexValues[indexValues.length - 1]));
            else
                rowIds.addAll(getAllRowIdsRightOf(page.pageNo, condition.comparisonValue));
        }

        return rowIds;

    }

    private List<Integer> getAllRowIdsLeftOf(int pageNo, String indexValue) {
        List<Integer> rowIds = new ArrayList<>();
        if (pageNo == -1)
            return rowIds;
        Page page = new Page(this.binaryFile, pageNo);
        List<String> indexValues = Arrays.asList(page.getIndexValues().toArray(new String[page.getIndexValues().size()]));


        for (int i = 0; i < indexValues.size() && Condition.compare(indexValues.get(i), indexValue, page.indexValueDataType) < 0; i++) {

            rowIds.addAll(page.indexValuePointer.get(indexValues.get(i)).getIndexNode().rowids);
            addAllChildRowIds(page.indexValuePointer.get(indexValues.get(i)).leftPageNo, rowIds);

        }

        if (page.indexValuePointer.get(indexValue) != null)
            addAllChildRowIds(page.indexValuePointer.get(indexValue).leftPageNo, rowIds);


        return rowIds;
    }

    private List<Integer> getAllRowIdsRightOf(int pageNo, String indexValue) {

        List<Integer> rowIds = new ArrayList<>();

        if (pageNo == -1)
            return rowIds;
        Page page = new Page(this.binaryFile, pageNo);
        List<String> indexValues = Arrays.asList(page.getIndexValues().toArray(new String[page.getIndexValues().size()]));
        for (int i = indexValues.size() - 1; i >= 0 && Condition.compare(indexValues.get(i), indexValue, page.indexValueDataType) > 0; i--) {
            rowIds.addAll(page.indexValuePointer.get(indexValues.get(i)).getIndexNode().rowids);
            addAllChildRowIds(page.rightPage, rowIds);
        }

        if (page.indexValuePointer.get(indexValue) != null)
            addAllChildRowIds(page.indexValuePointer.get(indexValue).rightPageNo, rowIds);

        return rowIds;
    }

    private void addAllChildRowIds(int pageNo, List<Integer> rowIds) {
        if (pageNo == -1)
            return;
        Page page = new Page(this.binaryFile, pageNo);
        for (Record record : page.indexValuePointer.values()) {
            rowIds.addAll(record.rowIds);
            if (page.pageType == PageType.INTERIORINDEX) {
                addAllChildRowIds(record.leftPageNo, rowIds);
                addAllChildRowIds(record.rightPageNo, rowIds);
            }
        }
    }

    public void insert(Field field, List<Integer> rowIds) {
        try {
            int pageNo = getClosestPageNo(root, field.fieldValue);
            Page page = new Page(binaryFile, pageNo);
            page.addIndex(new Node(field, rowIds));
        } catch (IOException e) {
            System.out.println("Error while inserting " + field.fieldValue + " into index file");
        }
    }

    public void insert(Field field, int rowId) {
        insert(field, Arrays.asList(rowId));
    }

    public void delete(Field field, int rowid) {

        try {
            int pageNo = getClosestPageNo(root, field.fieldValue);
            Page page = new Page(binaryFile, pageNo);

            Node tempNode = page.indexValuePointer.get(field.fieldValue).getIndexNode();
            tempNode.rowids.remove((Integer) rowid);

            page.DeleteIndex(tempNode);
            if (tempNode.rowids.size() != 0)
                page.addIndex(tempNode);

        } catch (IOException e) {
            System.out.println("Error while deleting " + field.fieldValue + " from index file");
        }

    }

    private String binarySearch(String[] values, String searchValue, int start, int end, DataType dataType) {

        if (end - start <= 3) {
            int i = start;
            for (i = start; i < end; i++) {
                if (Condition.compare(values[i], searchValue, dataType) < 0)
                    continue;
                else
                    break;
            }
            return values[i];
        } else {

            int mid = (end - start) / 2 + start;
            if (values[mid].equals(searchValue))
                return values[mid];

            if (Condition.compare(values[mid], searchValue, dataType) < 0)
                return binarySearch(values, searchValue, mid + 1, end, dataType);
            else
                return binarySearch(values, searchValue, start, mid - 1, dataType);

        }

    }


}