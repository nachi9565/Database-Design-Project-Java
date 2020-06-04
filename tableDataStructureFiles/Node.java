package tableDataStructureFiles;

import java.util.List;

import tableFiles.Field;

public class Node {
    public Field indexValue;
    public List<Integer> rowids;
    public boolean isInteriorNode;
    public int leftPageNo;

    public Node(Field indexValue, List<Integer> rowids) {
        this.indexValue = indexValue;
        this.rowids = rowids;
    }

}