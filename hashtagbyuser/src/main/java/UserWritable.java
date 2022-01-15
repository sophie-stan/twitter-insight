import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableUtils.readString;
import static org.apache.hadoop.io.WritableUtils.writeString;

public class UserWritable implements WritableComparable<UserWritable>, Cloneable {
    Long userId;
    String userName;
    //    int followersCount;

    public UserWritable() {
    }

    public UserWritable(Long userId, String userName) {
        this.userId = userId;
        this.userName = userName;
//        this.followersCount = followersCount;
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
        userId = in.readLong();
        userName = readString(in);
//        followersCount = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(userId);
        writeString(out, userName);
//        out.writeInt(followersCount);
    }

    @Override
    public String toString() {
        return String.format("(%d, %s)", userId, userName);
    }

    @Override
    public int compareTo(UserWritable o) {
        return Long.compare(this.userId, o.userId);
    }
}
