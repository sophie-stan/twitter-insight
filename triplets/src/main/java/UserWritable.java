import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableUtils.*;

public class UserWritable  implements Writable, Cloneable{
    String user;
    String[] hashtags;

    public UserWritable() {
    }

    public UserWritable(String user, String[] hashtags) {
        this.user = user;
        this.hashtags = hashtags;
    }

    @Override
    public UserWritable clone() {
        try {
            UserWritable tmp = (UserWritable) super.clone();
            tmp.hashtags = hashtags.clone();
            return tmp;
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user = readString(in);
        hashtags = readStringArray(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeString(out, user);
        writeStringArray(out, hashtags);
    }
}
