import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableUtils.readString;
import static org.apache.hadoop.io.WritableUtils.writeString;

public class UserWritable implements Writable, Cloneable {
    String userName;
    //    int followersCount;
    String hashtags;

    public UserWritable() {
    }

    public UserWritable(String userName, String hashtags) {
        this.userName = userName;
//        this.followersCount = followersCount;
        this.hashtags = hashtags;
    }

    @Override
    public UserWritable clone() {
        try {
            return (UserWritable) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userName = readString(in);
//        followersCount = in.readInt();
        hashtags = readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeString(out, userName);
//        out.writeInt(followersCount);
        writeString(out, hashtags);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", userName, hashtags);
    }
}
