package pdc.common;

public class MessageEnvelope {
    private MessageType type;
    private String requestId;
    private Object payload;
    private long timestamp;

    public MessageEnvelope() {
    }

    public MessageEnvelope(MessageType type, String requestId, Object payload) {
        this.type = type;
        this.requestId = requestId;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
