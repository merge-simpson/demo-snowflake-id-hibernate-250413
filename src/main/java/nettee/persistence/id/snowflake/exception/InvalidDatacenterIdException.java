package nettee.persistence.id.snowflake.exception;

import static nettee.persistence.id.snowflake.SnowflakeConstants.SnowflakeDefault.MAX_DATACENTER_ID;

public class InvalidDatacenterIdException extends RuntimeException {

    private final long datacenterId;

    public InvalidDatacenterIdException(long datacenterId) {
        super("Datacenter ID can't be greater than %d or less than 0. Input: %d"
                .formatted(MAX_DATACENTER_ID, datacenterId));
        this.datacenterId = datacenterId;
    }

    public long getDatacenterId() {
        return datacenterId;
    }
}
