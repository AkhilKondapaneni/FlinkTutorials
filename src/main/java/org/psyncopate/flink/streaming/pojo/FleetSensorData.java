package org.psyncopate.flink.streaming.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.Arrays;
import java.util.Objects;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FleetSensorData {
    private Schema schema;
    private Payload payload;

    // Getters and setters
    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Payload getPayload() {
        return payload;
    }

    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "FltBk{" +
                "schema=" + schema +
                ", payload=" + payload +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FleetSensorData that = (FleetSensorData) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, payload);
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Schema {
        private String type;
        private Field[] fields;
        private boolean optional;
        private String name;

        // Getters and setters
        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Field[] getFields() {
            return fields;
        }

        public void setFields(Field[] fields) {
            this.fields = fields;
        }

        public boolean isOptional() {
            return optional;
        }

        public void setOptional(boolean optional) {
            this.optional = optional;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Schema{" +
                    "type='" + type + '\'' +
                    ", fields=" + Arrays.toString(fields) +
                    ", optional=" + optional +
                    ", name='" + name + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Schema schema = (Schema) o;
            return optional == schema.optional &&
                    Objects.equals(type, schema.type) &&
                    Arrays.equals(fields, schema.fields) &&
                    Objects.equals(name, schema.name);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(type, optional, name);
            result = 31 * result + Arrays.hashCode(fields);
            return result;
        }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Field {
        private String type;
        private boolean optional;
        private String field;

        // Getters and setters
        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public boolean isOptional() {
            return optional;
        }

        public void setOptional(boolean optional) {
            this.optional = optional;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        @Override
        public String toString() {
            return "Field{" +
                    "type='" + type + '\'' +
                    ", optional=" + optional +
                    ", field='" + field + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field1 = (Field) o;
            return optional == field1.optional &&
                    Objects.equals(type, field1.type) &&
                    Objects.equals(field, field1.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, optional, field);
        }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Payload {
        @JsonProperty("vehicle_id")
        private int vehicleId;

        @JsonProperty("engine_temperature")
        private int engineTemperature;

        @JsonProperty("average_rpm")
        private int averageRpm;

        // Getters and setters
        public int getVehicleId() {
            return vehicleId;
        }

        public void setVehicleId(int vehicleId) {
            this.vehicleId = vehicleId;
        }

        public int getEngineTemperature() {
            return engineTemperature;
        }

        public void setEngineTemperature(int engineTemperature) {
            this.engineTemperature = engineTemperature;
        }

        public int getAverageRpm() {
            return averageRpm;
        }

        public void setAverageRpm(int averageRpm) {
            this.averageRpm = averageRpm;
        }

        @Override
        public String toString() {
            return "Payload{" +
                    "vehicleId=" + vehicleId +
                    ", engineTemperature=" + engineTemperature +
                    ", averageRpm=" + averageRpm +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Payload payload = (Payload) o;
            return vehicleId == payload.vehicleId &&
                    engineTemperature == payload.engineTemperature &&
                    averageRpm == payload.averageRpm;
        }

        @Override
        public int hashCode() {
            return Objects.hash(vehicleId, engineTemperature, averageRpm);
        }
    }

}