package tableFiles;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.nio.charset.StandardCharsets;

public class Field {
    public byte[] fieldValuebyte;
    public Byte[] fieldValueByte;

    public DataType dataType;

    public String fieldValue;

    public Field(DataType dataType, byte[] fieldValue) {
        this.dataType = dataType;
        this.fieldValuebyte = fieldValue;
        try {
            switch (dataType) {
                case NULL:
                    this.fieldValue = "NULL";
                    break;
                case TINYINT:
                    this.fieldValue = Byte.valueOf(ByteUtil.byteFromByteArray(fieldValuebyte)).toString();
                    break;
                case SMALLINT:
                    this.fieldValue = Short.valueOf(ByteUtil.shortFromByteArray(fieldValuebyte)).toString();
                    break;
                case INT:
                    this.fieldValue = Integer.valueOf(ByteUtil.intFromByteArray(fieldValuebyte)).toString();
                    break;
                case BIGINT:
                    this.fieldValue = Long.valueOf(ByteUtil.longFromByteArray(fieldValuebyte)).toString();
                    break;
                case FLOAT:
                    this.fieldValue = Float.valueOf(ByteUtil.floatFromByteArray(fieldValuebyte)).toString();
                    break;
                case DOUBLE:
                    this.fieldValue = Double.valueOf(ByteUtil.doubleFromByteArray(fieldValuebyte)).toString();
                    break;
                case YEAR:
                    this.fieldValue = Integer.valueOf((int) Byte.valueOf(ByteUtil.byteFromByteArray(fieldValuebyte)) + 2000).toString();
                    break;
                case TIME:
                    // HH:MM:SS
                    int millisSinceMidnight = ByteUtil.intFromByteArray(fieldValuebyte) % 86400000;
                    int seconds = millisSinceMidnight / 1000;
                    int hours = seconds / 3600;
                    int remHourSeconds = seconds % 3600;
                    int minutes = remHourSeconds / 60;
                    int remSeconds = remHourSeconds % 60;
                    this.fieldValue = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remSeconds);
                    break;
                case DATETIME:
                    // YYYY-MM-DD_HH:MM:SS
                    Date rawdatetime = new Date(Long.valueOf(ByteUtil.longFromByteArray(fieldValuebyte)));
                    this.fieldValue = String.format("%02d", rawdatetime.getYear() + 1900) + "-" + String.format("%02d", rawdatetime.getMonth() + 1)
                            + "-" + String.format("%02d", rawdatetime.getDate()) + "_" + String.format("%02d", rawdatetime.getHours()) + ":"
                            + String.format("%02d", rawdatetime.getMinutes()) + ":" + String.format("%02d", rawdatetime.getSeconds());
                    break;
                case DATE:
                    // YYYY-MM-DD
                    Date rawdate = new Date(Long.valueOf(ByteUtil.longFromByteArray(fieldValuebyte)));
                    this.fieldValue = String.format("%02d", rawdate.getYear() + 1900) + "-" + String.format("%02d", rawdate.getMonth() + 1)
                            + "-" + String.format("%02d", rawdate.getDate());
                    break;
                case TEXT:
                    this.fieldValue = new String(fieldValuebyte, StandardCharsets.UTF_8);
                    break;
                default:
                    this.fieldValue = new String(fieldValuebyte, StandardCharsets.UTF_8);
                    break;
            }
            this.fieldValueByte = ByteUtil.byteToBytes(fieldValuebyte);
        } catch (Exception ex) {
            System.out.println("Formatting exception");
        }

    }

    public Field(DataType dataType, String fieldValue) throws Exception {
        this.dataType = dataType;
        this.fieldValue = fieldValue;

        try {
            switch (dataType) {
                case NULL:
                    this.fieldValuebyte = null;
                    break;
                case TINYINT:
                    this.fieldValuebyte = new byte[]{Byte.parseByte(fieldValue)};
                    break;
                case SMALLINT:
                    this.fieldValuebyte = ByteUtil.shortTobytes(Short.parseShort(fieldValue));
                    break;
                case INT:
                    this.fieldValuebyte = ByteUtil.intTobytes(Integer.parseInt(fieldValue));
                    break;
                case BIGINT:
                    this.fieldValuebyte = ByteUtil.longTobytes(Long.parseLong(fieldValue));
                    break;
                case FLOAT:
                    this.fieldValuebyte = ByteUtil.floatTobytes(Float.parseFloat(fieldValue));
                    break;
                case DOUBLE:
                    this.fieldValuebyte = ByteUtil.doubleTobytes(Double.parseDouble(fieldValue));
                    break;
                case YEAR:
                    this.fieldValuebyte = new byte[]{(byte) (Integer.parseInt(fieldValue) - 2000)};
                    break;
                case TIME:
                    this.fieldValuebyte = ByteUtil.intTobytes(Integer.parseInt(fieldValue));
                    break;
                case DATETIME:
                    SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                    Date datetime = sdftime.parse(fieldValue);
                    this.fieldValuebyte = ByteUtil.longTobytes(datetime.getTime());
                    break;
                case DATE:
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = sdf.parse(fieldValue);
                    this.fieldValuebyte = ByteUtil.longTobytes(date.getTime());
                    break;
                case TEXT:
                    this.fieldValuebyte = fieldValue.getBytes();
                    break;
                default:
                    this.fieldValuebyte = fieldValue.getBytes(StandardCharsets.US_ASCII);
                    break;
            }
            this.fieldValueByte = ByteUtil.byteToBytes(fieldValuebyte);
        } catch (Exception e) {
            System.out.println("Cannot convert " + fieldValue + " to " + dataType.toString());
            throw e;
        }
    }

}