/**
 * @original author
 * Sophie Stan & Deborah Perreira
 * @modified by
 * David Auber
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableUtils.*;

public class TweetWritable implements Writable, Cloneable {
    long id;
    long timestamp;
    String text;
    String userName;
    // TODO: add userId
    int followersCount;
    boolean isRT;
    int retweetCount;
    String[] hashtags;

    public TweetWritable() {
    }

    public TweetWritable(long id, long timestamp, String text, String userName, int followersCount,
                         boolean isRT, int retweetCount, String[] hashtags) {
        this.id = id;
        this.timestamp = timestamp;
        this.text = text;
        this.userName = userName;
        this.followersCount = followersCount;
        this.isRT = isRT;
        this.retweetCount = retweetCount;
        this.hashtags = hashtags;
    }

    @Override
    public TweetWritable clone() {
        try {
            TweetWritable tmp = (TweetWritable) super.clone();
            tmp.hashtags = hashtags.clone();
            return tmp;
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        timestamp = in.readLong();
        text = readString(in);
        userName = readString(in);
        followersCount = in.readInt();
        isRT = in.readBoolean();
        retweetCount = in.readInt();
        hashtags = readStringArray(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(timestamp);
        writeString(out, text);
        writeString(out, userName);
        out.writeInt(followersCount);
        out.writeBoolean(isRT);
        out.writeInt(retweetCount);
        writeStringArray(out, hashtags);
    }
}
