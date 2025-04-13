package nettee.jpa.id.snowflake;

import nettee.persistence.id.snowflake.Snowflake;
import nettee.persistence.id.snowflake.SnowflakeProperties;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;

public class SnowflakeIdGenerator implements IdentifierGenerator {

    private final Snowflake snowflake; // delegation (위임)

    public SnowflakeIdGenerator(SnowflakeProperties properties) {
        this.snowflake = new Snowflake(properties);
    }

    @Override
    public Object generate(SharedSessionContractImplementor sharedSessionContractImplementor, Object o) {
        return snowflake.nextId();
    }
}
