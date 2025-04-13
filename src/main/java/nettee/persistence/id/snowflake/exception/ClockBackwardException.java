package nettee.persistence.id.snowflake.exception;

public class ClockBackwardException extends RuntimeException {

    private final long timestamp;
    private final long lastTimestamp;

    public ClockBackwardException(long timestamp, long lastTimestamp) {
        super("Clock moved backwards. Refusing to generate id for %d milliseconds".formatted(lastTimestamp - timestamp));
        this.timestamp = timestamp;
        this.lastTimestamp = lastTimestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }
}
