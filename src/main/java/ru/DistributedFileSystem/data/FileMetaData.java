package ru.DistributedFileSystem.data;

import java.sql.Timestamp;

public class FileMetaData {
    private Integer nodeId;
    private Long loadId;
    private Timestamp expires_at;

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    public void setLoadId(Long loadId) {
        this.loadId = loadId;
    }

    public Timestamp getExpires_at() {
        return expires_at;
    }

    public void setExpires_at(Timestamp expires_at) {
        this.expires_at = expires_at;
    }

    public FileMetaData(int nodeId, long loadId) {
        this.nodeId = nodeId;
        this.loadId = loadId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public long getLoadId() {
        return loadId;
    }

    public void setLoadId(long loadId) {
        this.loadId = loadId;
    }
}
