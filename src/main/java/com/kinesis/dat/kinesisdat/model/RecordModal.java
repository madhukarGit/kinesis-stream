package com.kinesis.dat.kinesisdat.model;

import java.util.List;
import java.util.UUID;

public class RecordModal {
    private String recordId;
    private String recordName;
    private List<String> recordData;

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getRecordName() {
        return recordName;
    }

    public void setRecordName(String recordName) {
        this.recordName = recordName;
    }

    public List<String> getRecordData() {
        return recordData;
    }

    public void setRecordData(List<String> recordData) {
        this.recordData = recordData;
    }
}
