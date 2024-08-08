package org.psyncopate.flink.stateful.operations.pojo;

import lombok.Data;

@Data
public class KafkaMessage {
    private Schema schema;
    private Transaction payload;

    @Data
    public static class Schema {
        private String type;
        private Field[] fields;
        private boolean optional;
        private String name;

        @Data
        public static class Field {
            private String type;
            private boolean optional;
            private String field;
        }
    }

    @Data
    public static class Transaction {
        private long transaction_id;
        private long card_id;
        private String user_id;
        private long purchase_id;
        private int store_id;
    }
}
