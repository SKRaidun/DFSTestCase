package ru.DistributedFileSystem.data;

public class FileMetaData {
    Integer nodeId;
    Long loadId;

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
