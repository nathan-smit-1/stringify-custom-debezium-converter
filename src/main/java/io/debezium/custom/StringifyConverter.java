package io.debezium.custom;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StringifyConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyConverter.class);

    private String columnToConvert; // The column name to look for

    @Override
    public void configure(Properties props) {
        // Read the column name to convert from the configuration
        columnToConvert = props.getProperty("column.name");
        LOGGER.info("StringifyConverter configured to convert column: {}", columnToConvert);
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        // Check if the current column matches the column we're supposed to convert
        if (column.name().equalsIgnoreCase(columnToConvert)) {
            LOGGER.info("StringifyConverter is converting column: {}", column.name());

            // Register a converter that converts the column's value to a string
            registration.register(SchemaBuilder.string().optional(), (input) -> {
                // Log the raw input value before transformation
                if (input == null) {
                    LOGGER.info("Raw value for column '{}' is null", column.name());
                    return null; // Handle nulls
                } else {
                    LOGGER.info("Raw value for column '{}' before transformation: {}", column.name(), input);
                }

                // Convert the input to a string representation
                String convertedValue = input.toString();
                LOGGER.info("Converted value for column '{}' is: {}", column.name(), convertedValue);
                return convertedValue;
            });
        }
    }
}
