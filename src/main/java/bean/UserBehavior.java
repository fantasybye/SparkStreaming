package bean;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class UserBehavior {
    @JsonFormat
    private long user_id;

    @JsonFormat
    private long item_id;

    @JsonFormat
    private long category_id;

    @JsonFormat
    private String behavior;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private Date ts;

    public UserBehavior() {
    }

    public UserBehavior(long user_id, long item_id, long category_id, String behavior, Date ts) {
        this.user_id = user_id;
        this.item_id = item_id;
        this.category_id = category_id;
        this.behavior = behavior;
        this.ts = ts;
    }
}
