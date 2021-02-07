package com.bonyansystem.processors.asn1;

import java.io.BufferedOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ASN1RecordSet extends ArrayList<byte[][]> {
    static Logger logger = Logger.getLogger("com.bonyansystem");
    private Map<String, ASN1Header> headerMap; //last cell filled in a column
    private byte[][] masterRow;
    private DataType[] dataTypes;
    //private final String iterationTag;
    private int columnCount;

    public ASN1RecordSet(String recordSchema, String schemaDataTypes) throws Exception {
        initHeaderMap(recordSchema);

        this.masterRow = new byte[headerMap.size()][];
        this.columnCount = headerMap.size();
        this.dataTypes = new DataType[columnCount];

        setSchemaDataTypes(schemaDataTypes);
    }

    private void initHeaderMap(String recordSchema) throws Exception {
        headerMap = new HashMap<>();
        int explicitSequence;
        int colNum = 0;
        Pattern pattern = Pattern.compile("\\[\\d*\\]");

        for (String s : recordSchema.split(",")) {
            Matcher matcher = pattern.matcher(s);
            String seqStr = "";
            explicitSequence = -1;
            if (matcher.find()) {
                seqStr = matcher.group(0);
                explicitSequence = Integer.parseInt(seqStr
                        .replace("[", "")
                        .replace("]", ""));
                s = s.replace(seqStr, "");
                logger.finest("Found explicit sequence column: " + s + " Sequence: " + explicitSequence);
            }
            ASN1Header header = new ASN1Header();
            header.setHeaderName(s);
            header.setCurrentRow(0);
            header.setExplicitSequence(explicitSequence);
            header.setColNum(colNum);
            headerMap.put(s, header);
            colNum++;
        }
    }

    public void purge() {
        Arrays.fill(masterRow, null);
        for (Map.Entry e : headerMap.entrySet())
            ((ASN1Header) e.getValue()).setCurrentRow(0);
        this.clear();
    }

    public void setSchemaDataTypes(String schemaDataTypes) throws Exception {
        String[] dataTypesRaw = schemaDataTypes.split(",");
        if (dataTypesRaw.length != columnCount)
            throw new Exception("Invalid data types string.");

        for (int i = 0; i < columnCount; i++)
            this.dataTypes[i] = DataType.valueOf(dataTypesRaw[i]);
    }

    public int getHeaderNum(String header) throws Exception {
        return headerMap.get(header).getColNum();
    }

    public boolean hasHeader(String header) {
        return headerMap.containsKey(header);
    }

    public void populateColumn(String header, byte[] value) throws Exception {
        int num = getHeaderNum(header);
        for (byte[][] b : this)
            b[num] = value;
        if (this.size() < 1)
            throw new Exception("No record is available.");
    }

    private void populateMasterCell(String tag, byte[] value) throws Exception {
        int num = getHeaderNum(tag);

        if (masterRow[num] == null)
            masterRow[num] = value;
        else
            throw new Exception("Master cell is not empty. header=" + tag + " col=" + num);
    }

    private void populateIteratedCell(String tag, byte[] value) throws Exception {
        try {
            int currentCol = getHeaderNum(tag);
            int currentRow = headerMap.get(tag).getCurrentRow();
            if (currentRow + 1 > this.size())
                addEmptyRow();

            if (get(currentRow)[currentCol] == null) {
                get(currentRow)[currentCol] = value;
                headerMap.get(tag).setCurrentRow(++currentRow);
            } else
                throw new Exception("Trying to overwrite iterated cell.");
        } catch (Exception e) {
            logger.severe("ERROR: Populating iterated cell.");
            e.printStackTrace();
            throw e;
        }
    }

    public void populateCell(String tag, byte[] value) throws Exception {
        if (tag.contains("*")) {
            populateIteratedCell(tag, value);
        } else
            populateMasterCell(tag, value);
    }

    public String toString() {
        String[] rows = new String[0];
        try {
            rows = getDecodedRows();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String result = "";
        for (String s : rows) {
            result += (result.length() == 0 ? "@" : "\n\r") + s;
        }
        return Arrays.toString(rows);
    }

    public int buildRecords(int initialRecordNum) throws Exception {
        if (size() == 0) addEmptyRow();
        logger.fine("Building records. Index: " + initialRecordNum);
        try {
            if (hasHeader("SUB_SEQ")) {
                int colNum = getHeaderNum("SUB_SEQ");
                for (Integer row = 1; row <= this.size(); row++) {
                    byte[] val = ByteBuffer.allocate(4).putInt(row).array();
                    get(row - 1)[colNum] = val;
                }
            }

            if (hasHeader("REC_NO")) {
                int colNum = getHeaderNum("REC_NO");
                int recNo = initialRecordNum;
                for (Integer row = 1; row <= this.size(); row++) {
                    recNo += 1;
                    byte[] val = ByteBuffer.allocate(4).putInt(recNo).array();
                    get(row - 1)[colNum] = val;
                }
            }
        } catch (Exception e) {
            throw new Exception("Error populating ID columns");
        }

        for (byte[][] row : this) {
            for (int i = 0; i < columnCount; i++) {
                if (masterRow[i] != null) {
                    if (row[i] == null)
                        row[i] = masterRow[i];
                    else
                        throw new Exception("Cell is not empty. row=" + this.indexOf(row) + " col=" + i);
                }
            }
        }
        populateExplicitSequenceRecords();

        return size();
    }

    private void populateExplicitSequenceRecords() throws Exception {
        byte[][] row;
        for (ASN1Header h : headerMap.values()) {
            if (h.getExplicitSequence() > -1 && h.getExplicitSequence() < size()) {
                int colNum = getHeaderNum(h.getHeaderName());
                row = get(h.getExplicitSequence());
                for (byte[][] r : this) {
                    if (!r.equals(row))
                        r[colNum] = row[colNum].clone();
                }
            }
        }
    }

    public void writeRecords(BufferedOutputStream bufferedOutputStream) throws Exception {
        logger.fine("Writing records to the buffer.");
        String[] rows = new String[this.size()];
        String str;
        for (byte[][] row : this) {
            str = "";
            for (int i = 0; i < columnCount; i++) {
                str += (i==0 ? "" : ",");
                if (row[i] != null) {
                    str += decodeData(row[i], dataTypes[i]);
                } else
                    str += "";
            }
            str += System.lineSeparator();
            bufferedOutputStream.write(str.getBytes());
        }
        logger.fine("Buffer write successfull.");
    }

    public String[] getDecodedRows() throws Exception {
        String[] rows = new String[this.size()];
        for (byte[][] row : this) {
            StringBuilder csvRow = new StringBuilder();
            for (int i = 0; i < columnCount; i++) {
                String str = decodeData(row[i], dataTypes[i]);
                csvRow.append(csvRow.length() == 0 ? "" : ",").append(str);
            }
            rows[indexOf(row)] = csvRow.toString();
        }
        return rows;
    }

    public void addEmptyRow() {
        add(new byte[headerMap.size()][]);
    }

    public String decodeData(byte[] data, DataType dataType) throws Exception {
        String decoded = "";
        switch (dataType) {
            case OCTET_STRING:
                for (byte b : data)
                    decoded += String.format("%02X", b);
                break;
            case TBCD_STRING:
                for (byte b : data) { //Convert signed byte to unsigned short by (& 0xff)
                    if (((b & 0xff) & 0xf) != 0xf)
                        decoded += Integer.toString((b & 0xff) & 0xf);
                    if ((((b & 0xff) >> 4) & 0xf) != 0xf)
                        decoded += Integer.toString((b & 0xff) >> 4);
                }
                break;
            case IA5_STRING:
                decoded = "\"" + new String(data, StandardCharsets.UTF_8).replace("\"", "\"\"") + "\"";
                break;
            case INTEGER:
                decoded = new BigInteger(data).toString();
                break;
            case IP_STRING:
                if (data.length != 4)
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
                for (int i = 0; i < 8; i++) {
                    int a1 = Integer.parseInt(String.valueOf(buff.charAt(index)), 16);
                    int a2 = Integer.parseInt(String.valueOf(buff.charAt(++index)), 16);
                    int a3 = Integer.parseInt(String.valueOf(buff.charAt(++index)), 16);
                    index++;

                    int q = a2 / 4;
                    int r = a2 % 4;
                    s[0] = (4 * a1) + q; //q = a2 / 4;
                    s[1] = (16 * r) + a3;//r = a2 % 4;

                    for (int c : s) {
                        if (c >= 0 && c < 10) {
                            decoded += (char) (c + 48);
                        } else if (c < 36) {
                            decoded += (char) (c + 55);
                        } else if (c < 62) {
                            decoded += (char) (c + 61);
                        } else if (c == 62) {
                            decoded += "!";
                        } else if (c == 63) {
                            decoded += "_";
                        } else {
                            throw new Exception("Invalid IPV6_STRING char value. index= " + index + " char= " + c);
                        }
                        buff += (char) c;
                    }
                }
                break;
            case BOOLEAN:
                decoded = (data[0] == (byte) 0xff ? "TRUE" : "FALSE");
                break;
            default:
                throw new Exception("Data type is invalid. dataType=" + dataType);
        }
        return decoded;
    }
}
