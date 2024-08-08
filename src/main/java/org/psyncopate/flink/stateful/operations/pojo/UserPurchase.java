package org.psyncopate.flink.stateful.operations.pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

@Data
public class UserPurchase {
    private String userId;
    private int totalPurchases;
    private long windowStart;
    private long windowEnd;

    public UserPurchase() {}

    public UserPurchase(String userId, int totalPurchases, long windowStart, long windowEnd) {
        this.userId = userId;
        this.totalPurchases = totalPurchases;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public String toJson() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert UserPurchase to JSON", e);
        }
    }

}
