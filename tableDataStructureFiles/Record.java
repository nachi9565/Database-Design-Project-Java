package tableDataStructureFiles;

import java.util.List;

import tableFiles.ByteUtil;
import tableFiles.DataType;
import tableFiles.Field;

public class Record {
    public Byte noOfRowIds;
    public DataType dataType;
    public Byte[] indexValue;
    public List<Integer> rowIds;
    public short pageHeaderIndex;
    public short pageOffset;
    public int leftPageNo;
    public int rightPageNo;
    int pageNo;
    private Node indexNode;


    public Record(short pageHeaderIndex, DataType dataType, Byte NoOfRowIds, byte[] indexValue, List<Integer> rowIds
            , int leftPageNo, int rightPageNo, int pageNo, short pageOffset) {

        this.pageOffset = pageOffset;
        this.pageHeaderIndex = pageHeaderIndex;
        this.noOfRowIds = NoOfRowIds;
        this.dataType = dataType;
        this.indexValue = ByteUtil.byteToBytes(indexValue);
        this.rowIds = rowIds;

        indexNode = new Node(new Field(this.dataType, indexValue), rowIds);
        this.leftPageNo = leftPageNo;
        this.rightPageNo = rightPageNo;
        this.pageNo = pageNo;
    }

    public Node getIndexNode() {
        return indexNode;
    }


}