package nettee.jpa.id.snowflake;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(
        schema = "sample",
        name = "sample"
)
public class Sample {
    @Id
    @SnowflakeIdGenerated
    public long id;
}
