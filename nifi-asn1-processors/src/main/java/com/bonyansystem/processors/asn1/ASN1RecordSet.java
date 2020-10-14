package com.bonyansystem.processors.asn1;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class ASN1RecordSet extends ArrayList<byte[][]> {

    private final String[] headers;
    private final byte[][] masterRow;
    private final DataType[] dataTypes;
    private final String iterationTag;
    private final int columnCount;

    public ASN1RecordSet(String recordSchema, String iterationTag, String schemaDataTypes) throws Exception {
        this.headers = recordSchema.split(",");
        this.iterationTag = iterationTag;
        this.masterRow = new byte[headers.length][];
        this.columnCount = headers.length;
        this.dataTypes = new DataType[columnCount];

        setSchemaDataTypes(schemaDataTypes);
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
        for (String s : headers) {
            if (s.equals(header))
                return true;
        }
        return false;
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
        int num = getHeaderNum(tag);

        if(get(size() - 1)[num] == null)
            get(size() - 1)[num] = value;
        else
            throw new Exception("Trying to overwrite iterated cell.");
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

    public int buildRecords() throws Exception{
        if(size()==0) addEmptyRow();
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
    }

    public void purge() {
        Arrays.fill(masterRow, null );
        this.clear();
    }

    public String decodeData(byte[] data, DataType dataType) throws Exception {
        String decoded = "";
        switch (dataType){
            case OCTET_STRING:
                for (byte b : data)
                    decoded += String.format("%02X", b);
                break;
            case TBCD_STRING:
                for (byte b : data)//Convert signed byte to unsigned short by (& 0xff)
                    decoded += Integer.toString((b & 0xff) & 0xf) + Integer.toString((b & 0xff)>>4);
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
            default:
                throw new Exception("Data type is invalid. dataType=" + dataType);
        }
        return decoded;
    }
}
