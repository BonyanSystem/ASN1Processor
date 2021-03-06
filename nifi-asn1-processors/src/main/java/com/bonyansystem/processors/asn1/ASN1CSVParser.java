package com.bonyansystem.processors.asn1;

import java.io.*;
import java.util.logging.Logger;

public class ASN1CSVParser implements BERTags {
    static Logger logger = Logger.getLogger("com.bonyansystem");
    private final BufferedInputStream inputStream;
    private final String schema;
    private final String schemaDataTypes;
    private ASN1RecordSet recordSet;
    private int level;
    private int pos = 1;
    private String lastSequenceAddress = "";
    private int recordSeq = 0;

    public ASN1CSVParser(BufferedInputStream bufferedInputStream,
                         String schema, String schemaDataTypes) throws Exception {
        this.inputStream = bufferedInputStream;
        this.schema = schema;
        this.schemaDataTypes = schemaDataTypes;

        recordSet = new ASN1RecordSet(schema, schemaDataTypes);
    }

    public int parse(BufferedOutputStream bufferedOutputStream) throws Exception {
        int recordCount = 0;
        while (inputStream.available() > 0) {
            digIn("", 0, false);

            recordCount += recordSet.buildRecords(recordCount);
            recordSet.writeRecords(bufferedOutputStream);
            recordSet.purge();
        }
        logger.info("Parse completed.");
        return recordCount;
    }

    int digIn(String parentAddress, int parentSequence, boolean isIterationTag) throws Exception {
        int tag = inputStream.read();
        pos++;
        int tagNo = readTagNumber(tag);
        int length = readLength(inputStream, 99999, true);
        String currentAddress = parentAddress + (parentAddress.length()==0?"":".") + tagNo;

        boolean isConstructed = (tag & CONSTRUCTED) != 0;// PRIMITIVE or CONSTRUCTED
        if (isConstructed) {
            switch (tag & 0xC0) {
                case UNIVERSAL:
                    switch (tagNo) {
                        case SEQUENCE:
                            parseDefiniteBlock(length, parentAddress + (parentAddress.length()==0?"":".") + "*");
                            return tagNo;
                        default:
                            throw new Exception("UNIVERSAL data type is invalid.");
                    }
                case TAGGED:
                    parseDefiniteBlock(length, currentAddress);
                    return tagNo;
                case APPLICATION:
                    throw new Exception("Tag APPLICATION is not defined in this context.");
                case PRIVATE:
                    throw new Exception("Tag PRIVATE is not defined in this context.");
            }
            throw new IOException("Invalid tag");

        } else {//TAG is primitive
            readPrimitive(currentAddress, length);
            return tagNo;
        }
    }

    private void parseDefiniteBlock(int length, String address) throws Exception {
        int startPos = pos;
        int seq = 0;
        level++;
        while (pos < startPos + length) {
            seq++;
            digIn(address, seq, address.contains("*"));
            if(address.contains("*"))
                logger.finest("Address: " + address + "  Sequence: " + seq);
        }
        level--;
    }

    void readPrimitive(String address, int length) throws Exception {
        byte[] data = new byte[length];
        pos += length;

        if(inputStream.read(data) != length)
            throw new Exception("Corrupted data block. pos: " + pos);

        if(recordSet.hasHeader(address))
            recordSet.populateCell(address, data);
    }

    int readTagNumber(int tag) throws IOException {
        int tagNo = tag & 0x1f;

        //
        // with tagged object tag number is bottom 5 bits, or stored at the start of the content
        //
        if (tagNo == 0x1f) {
            tagNo = 0;

            int b = inputStream.read();
            pos++;

            // X.690-0207 8.1.2.4.2
            // "c) bits 7 to 1 of the first subsequent octet shall not all be zero."
            if ((b & 0x7f) == 0) // Note: -1 will pass
            {
                throw new IOException("corrupted stream - invalid high tag number found");
            }

            while ((b >= 0) && ((b & 0x80) != 0)) {
                tagNo |= (b & 0x7f);
                tagNo <<= 7;
                b = inputStream.read();
                pos++;
            }

            if (b < 0) {
                throw new EOFException("EOF found inside tag value.");
            }

            tagNo |= (b & 0x7f);
        }
        return tagNo;
    }

    int readLength(InputStream s, int limit, boolean isParsing) throws IOException {
        int length = s.read();
        pos++;

        if (length < 0) {
            throw new EOFException("EOF found when length expected");
        }

        if (length == 0x80) {
            return -1;      // indefinite-length encoding
        }

        if (length > 127) {
            int size = length & 0x7f;

            // Note: The invalid long form "0xff" (see X.690 8.1.3.5c) will be caught here
            if (size > 4) {
                throw new IOException("DER length more than 4 bytes: " + size);
            }

            length = 0;
            for (int i = 0; i < size; i++) {
                int next = s.read();
                pos++;

                if (next < 0) {
                    throw new EOFException("EOF found reading length");
                }

                length = (length << 8) + next;
            }

            if (length < 0) {
                throw new IOException("corrupted stream - negative length found");
            }

            if (length >= limit && !isParsing)   // after all we must have read at least 1 byte
            {
                throw new IOException("corrupted stream - out of bounds length found: " + length + " >= " + limit);
            }
        }
        return length;
    }
}
