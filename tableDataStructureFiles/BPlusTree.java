package tableDataStructureFiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import baseFile.DavisBase;
import tableFiles.Condition;
import tableFiles.OperatorType;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

public class BPlusTree {

    RandomAccessFile binaryFile;
    int rootPageNo;
    String tableName;

    public BPlusTree(RandomAccessFile file, int rootPageNo, String tableName) {
        this.binaryFile = file;
        this.rootPageNo = rootPageNo;
        this.tableName = tableName;
    }

    public List<Integer> getAllLeaves() throws IOException {

        List<Integer> leafPages = new ArrayList<>();
        binaryFile.seek(rootPageNo * DavisBaseBinaryFile.pageSize);
        // if root is leaf page read directly return. no traversal required
        PageType rootPageType = PageType.get(binaryFile.readByte());
        if (rootPageType == PageType.LEAF) {
            if (!leafPages.contains(rootPageNo))
                leafPages.add(rootPageNo);
        } else {
            addLeaves(rootPageNo, leafPages);
        }

        return leafPages;

    }

    private void addLeaves(int interiorPageNo, List<Integer> leafPages) throws IOException {
        Page interiorPage = new Page(binaryFile, interiorPageNo);
        for (TableInteriorRecord leftPage : interiorPage.leftChildren) {
            if (Page.getPageType(binaryFile, leftPage.leftChildPageNo) == PageType.LEAF) {
                if (!leafPages.contains(leftPage.leftChildPageNo))
                    leafPages.add(leftPage.leftChildPageNo);
            } else {
                addLeaves(leftPage.leftChildPageNo, leafPages);
            }
        }

        if (Page.getPageType(binaryFile, interiorPage.rightPage) == PageType.LEAF) {
            if (!leafPages.contains(interiorPage.rightPage))
                leafPages.add(interiorPage.rightPage);
        } else {
            addLeaves(interiorPage.rightPage, leafPages);
        }

    }

    public List<Integer> getAllLeaves(Condition condition) throws IOException {

        if (condition == null || condition.getOperation() == OperatorType.NOTEQUAL
                || !(new File(DavisBase.getNDXFilePath(tableName, condition.columnName)).exists())) {
            return getAllLeaves();
        } else {

            RandomAccessFile indexFile = new RandomAccessFile(
                    DavisBase.getNDXFilePath(tableName, condition.columnName), "r");
            Tree bTree = new Tree(indexFile);

            List<Integer> rowIds = bTree.getRowIds(condition);
            Set<Integer> hash_Set = new HashSet<>();

            for (int rowId : rowIds) {
                hash_Set.add(getPageNo(rowId, new Page(binaryFile, rootPageNo)));
            }

            for (int rowId : rowIds) {
                System.out.print(" " + rowId + " ");
            }

            System.out.println();
            System.out.println(" leaves: " + hash_Set);
            System.out.println();

            indexFile.close();

            return Arrays.asList(hash_Set.toArray(new Integer[hash_Set.size()]));
        }

    }

    public static int getPageNoForInsert(RandomAccessFile file, int rootPageNo) {
        Page rootPage = new Page(file, rootPageNo);
        if (rootPage.pageType != PageType.LEAF && rootPage.pageType != PageType.LEAFINDEX)
            return getPageNoForInsert(file, rootPage.rightPage);
        else
            return rootPageNo;

    }

    public int getPageNo(int rowId, Page page) {
        if (page.pageType == PageType.LEAF)
            return page.pageNo;

        int index = binarySearch(page.leftChildren, rowId, 0, page.noOfCells - 1);

        if (rowId < page.leftChildren.get(index).rowId) {
            return getPageNo(rowId, new Page(binaryFile, page.leftChildren.get(index).leftChildPageNo));
        } else {
            if (index + 1 < page.leftChildren.size())
                return getPageNo(rowId, new Page(binaryFile, page.leftChildren.get(index + 1).leftChildPageNo));
            else
                return getPageNo(rowId, new Page(binaryFile, page.rightPage));
        }
    }

    private int binarySearch(List<TableInteriorRecord> values, int searchValue, int start, int end) {

        if (end - start <= 2) {
            int i = start;
            for (i = start; i < end; i++) {
                if (values.get(i).rowId < searchValue)
                    continue;
                else
                    break;
            }
            return i;
        } else {

            int mid = (end - start) / 2 + start;
            if (values.get(mid).rowId == searchValue)
                return mid;

            if (values.get(mid).rowId < searchValue)
                return binarySearch(values, searchValue, mid + 1, end);
            else
                return binarySearch(values, searchValue, start, mid - 1);

        }

    }

}