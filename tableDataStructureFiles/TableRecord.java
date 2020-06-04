package tableDataStructureFiles;

import java.util.List;

import tableFiles.ByteUtil;
import tableFiles.DataType;
import tableFiles.Field;

import java.util.Arrays;
import java.util.ArrayList;

public class TableRecord {
    public int rowId;
    public Byte[] colDatatypes;
    public Byte[] recordBody;
    private List<Field> fields;
    public short recordOffset;
    public short pageHeaderIndex;

    public TableRecord(short pageHeaderIndex, int rowId, short recordOffset, byte[] colDatatypes, byte[] recordBody) {
        this.rowId = rowId;
        this.recordBody = ByteUtil.byteToBytes(recordBody);
        this.colDatatypes = ByteUtil.byteToBytes(colDatatypes);
        this.recordOffset = recordOffset;
        this.pageHeaderIndex = pageHeaderIndex;
        setAttributes();
    }

    public List<Field> getFields() {
        return fields;
    }

    private void setAttributes() {
        fields = new ArrayList<>();
        int pointer = 0;
        for (Byte colDataType : colDatatypes) {
            byte[] fieldValue = ByteUtil.Bytestobytes(Arrays.copyOfRange(recordBody, pointer, pointer + DataType.getLength(colDataType)));
            fields.add(new Field(DataType.get(colDataType), fieldValue));
            pointer = pointer + DataType.getLength(colDataType);
        }
    }

}