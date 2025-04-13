package nettee.jpa.id.snowflake;

import nettee.persistence.id.snowflake.SnowflakeProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = "nettee")
@ConfigurationPropertiesScan(basePackages = "nettee")
public class TestApplication {
    public static void main(String[] args) {
        var context = SpringApplication.run(TestApplication.class, args);
        var properties = context.getBean(SnowflakeProperties.class);
        System.out.println("SnowflakeProperties: " + properties);
    }
}
