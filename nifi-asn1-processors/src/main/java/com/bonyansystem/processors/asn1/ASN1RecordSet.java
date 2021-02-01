package com.bonyansystem.processors.asn1;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ASN1RecordSet extends ArrayList<byte[][]> {
    static Logger logger = Logger.getGlobal();
    private final String[] headers; //column names and positions
    private final Map<String, Integer> headersMap; //last cell filled in a column
    private final byte[][] masterRow;
    private final DataType[] dataTypes;
    //private final String iterationTag;
    private final int columnCount;
    private int iterateRecordSeq = 0;

    public ASN1RecordSet(String recordSchema, /*String iterationTag,*/ String schemaDataTypes) throws Exception {
        this.headers = recordSchema.split(",");
        this.headersMap = new HashMap<String, Integer>();
        for(String s : headers)
            headersMap.put(s, 0);
        this.masterRow = new byte[headers.length][];
        this.columnCount = headers.length;
        this.dataTypes = new DataType[columnCount];

        setSchemaDataTypes(schemaDataTypes);
    }

    public void purge() {
        Arrays.fill(masterRow, null );
        for(Map.Entry<String, Integer> e : headersMap.entrySet() )
            e.setValue(0);
        iterateRecordSeq = 0;
        this.clear();
    }

    public void setSchemaDataTypes(String schemaDataTypes) throws Exception {
        String[] dataTypesRaw = schemaDataTypes.split(",");
        if(dataTypesRaw.length != columnCount)
            throw new Exception("Invalid data types string.");

        for(int i=0; i<columnCount; i++)
            this.dataTypes[i] = DataType.valueOf(dataTypesRaw[i]);
    }

    public int getHeaderNum(String header) throws Exception {
        for (int i=0; i<headers.length; i++){
            if(headers[i].equals(header))
                return i;
        }
        throw new Exception("Column header not found.");
    }

    public boolean hasHeader(String header){
        return headersMap.containsKey(header);
    }

    public void populateColumn(String header, byte[] value) throws Exception {
        int num = getHeaderNum(header);
        for(byte[][] b : this)
            b[num] = value;
        if(this.size() < 1)
            throw new Exception("No record is available.");
    }

    public void populateMasterCell(String tag, byte[] value) throws Exception {
        int num = getHeaderNum(tag);

        if(masterRow[num] == null)
            masterRow[num] = value;
        else
            throw new Exception("Master cell is not empty. header=" + tag + " col=" + num );
    }

    public void populateIteratedCell(String tag, byte[] value) throws Exception {
        int currentCol = getHeaderNum(tag);
        int currentRow = headersMap.get(tag);
        if (currentRow + 1 > this.size())
            addEmptyRow();

        if(get(currentRow)[currentCol] == null){
            get(currentRow)[currentCol] = value;
            headersMap.replace(tag, ++currentRow);
        }else
            throw new Exception("Trying to overwrite iterated cell.");
    }

    public void populateCell(String tag, byte[] value) throws Exception{
        if (tag.contains("*")){
            populateIteratedCell(tag, value);
        }else
            populateMasterCell(tag, value);
    }

    public String toString(){
        String[] rows = new String[0];
        try {
            rows = getDecodedRows();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String result = "";
        for(String s : rows){
            result += (result.length()==0?"@":"\n\r") + s;
        }
        return Arrays.toString(rows);
    }

    public int buildRecords(int initialRecordNum) throws Exception{
        if(size()==0) addEmptyRow();

        if(hasHeader("SUB_SEQ")){
            int colNum = getHeaderNum("SUB_SEQ");
            for(Integer row=1; row<=this.size(); row++) {
                byte[] val = ByteBuffer.allocate(4).putInt(row).array();
                get(row - 1)[colNum] = val;
            }
        }
        if(hasHeader("REC_NO")){
            int colNum = getHeaderNum("REC_NO");
            int recNo = initialRecordNum;
            for(Integer row=1; row<=this.size(); row++) {
                recNo += 1;
                byte[] val = ByteBuffer.allocate(4).putInt(recNo).array();
                get(row - 1)[colNum] = val;
            }
        }
        for(byte[][] row : this){
            for(int i=0; i<columnCount; i++){
                if(masterRow[i] != null){
                    if(row[i] == null)
                        row[i] = masterRow[i];
                    else
                        throw new Exception("Cell is not empty. row=" + this.indexOf(row) + " col=" + i);
                }
            }
        }
        return size();
    }

    public void writeRecords(BufferedOutputStream bufferedOutputStream) throws Exception {
        String[] rows = new String[this.size()];
        String str;
        for(byte[][] row : this) {
            str = "";
            for (int i=0; i<columnCount; i++) {
                str += (str.length() == 0 ? "" : "," );
                if(row[i] != null) {
                    str += decodeData(row[i], dataTypes[i]);
                }else
                    str += "";
            }
            str += System.lineSeparator();
            bufferedOutputStream.write(str.getBytes());
        }
    }

    public String[] getDecodedRows() throws Exception {
        String[] rows = new String[this.size()];
        for(byte[][] row : this) {
            StringBuilder csvRow = new StringBuilder();
            for (int i=0; i<columnCount; i++) {
                String str = decodeData(row[i], dataTypes[i]);
                csvRow.append(csvRow.length() == 0 ? "" : ",").append(str);
            }
            rows[indexOf(row)] = csvRow.toString();
        }
        return rows;
    }

    public void addEmptyRow(){
        add(new byte[headers.length][]);
        iterateRecordSeq++;
        logger.info("iterateRecordSeq= " + iterateRecordSeq);
    }

    public String decodeData(byte[] data, DataType dataType) throws Exception {
        String decoded = "";
        switch (dataType){
            case OCTET_STRING:
                for (byte b : data)
                    decoded += String.format("%02X", b);
                break;
            case TBCD_STRING:
                for (byte b : data) { //Convert signed byte to unsigned short by (& 0xff)
                    if(((b & 0xff) & 0xf) != 0xf)
                        decoded += Integer.toString((b & 0xff) & 0xf);
                    if((((b & 0xff)>>4) & 0xf) != 0xf)
                        decoded += Integer.toString((b & 0xff)>>4);
                }
                break;
            case IA5_STRING:
                decoded = "\"" + new String(data, StandardCharsets.UTF_8).replace("\"", "\"\"") + "\"";
                break;
            case INTEGER:
                    decoded = new BigInteger(data).toString();
                break;
            case IP_STRING:
                if(data.length != 4)
                    throw new Exception("Invalid IP_STRING data.");
                decoded = Integer.toString(data[0] & 0xff) + "."
                        + Integer.toString(data[1] & 0xff) + "."
                        + Integer.toString(data[2] & 0xff) + "."
                        + Integer.toString(data[3] & 0xff);
                break;
            case IPV6_STRING:
                if (data.length != 16)
                    throw new Exception("Invalid IPV6_STRING length:" + data.length);
                String buff = "";

                for (byte b : data) //Convert signed byte to unsigned short by (& 0xff)
                    buff += String.format("%02X", b);

                int index = 8;
                int[] s = new int[2];
                for (int i=0; i<8; i++){
                    int a1 = Integer.parseInt(String.valueOf(buff.charAt(index)) , 16);
                    int a2 = Integer.parseInt(String.valueOf(buff.charAt(++index)), 16);
                    int a3 = Integer.parseInt(String.valueOf(buff.charAt(++index)), 16);
                    index++;

                    int q = a2 / 4;
                    int r = a2 % 4;
                    s[0] = (4 * a1) + q; //q = a2 / 4;
                    s[1] = (16 * r) + a3;//r = a2 % 4;

                    for(int c : s){
                        if(c >= 0 && c < 10){
                            decoded += (char)(c + 48);
                        }else if(c < 36){
                            decoded += (char)(c + 55);
                        }else if(c < 62){
                            decoded += (char)(c + 61);
                        }else if(c == 62){
                            decoded += "!";
                        }else if(c == 63) {
                            decoded += "_";
                        }else{
                            throw new Exception("Invalid IPV6_STRING char value. index= " + index + " char= " + c);
                        }
                        buff += (char) c;
                    }
                }
                break;
            default:
                throw new Exception("Data type is invalid. dataType=" + dataType);
        }
        return decoded;
    }
}
