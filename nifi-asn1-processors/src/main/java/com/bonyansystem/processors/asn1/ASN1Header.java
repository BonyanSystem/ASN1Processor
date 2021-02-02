package com.bonyansystem.processors.asn1;

public class ASN1Header {
    private String headerName;
    private int currentRow;
    private int explicitSequence;
    private int colNum;

    public String getHeaderName() {
        return headerName;
    }

    public void setHeaderName(String headerName) {
        this.headerName = headerName;
    }

    public int getCurrentRow() {
        return currentRow;
    }

    public void setCurrentRow(int currentRow) {
        this.currentRow = currentRow;
    }

    public int getExplicitSequence() {
        return explicitSequence;
    }

    public void setExplicitSequence(int explicitSequence) {
        this.explicitSequence = explicitSequence;
    }

    public int getColNum() {
        return colNum;
    }

    public void setColNum(int colNum) {
        this.colNum = colNum;
    }
}
