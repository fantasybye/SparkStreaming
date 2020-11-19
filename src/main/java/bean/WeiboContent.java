package bean;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class WeiboContent {
    @JsonFormat
    private String user;

    @JsonFormat
    private long uid;

    @JsonFormat
    private String content;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private Date ts;

    public WeiboContent(){}

    public WeiboContent(String user, long uid, String content, Date ts) {
        this.user = user;
        this.uid = uid;
        this.content = content;
        this.ts = ts;
    }
}
