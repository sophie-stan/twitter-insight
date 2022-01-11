import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class TweetMapper extends Mapper<LongWritable, Text, LongWritable, TweetWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject jsonObject = new JSONObject(value.toString());

            long id = jsonObject.getLong("id");
            long timestamp = jsonObject.getLong("timestamp_ms");
            String text = jsonObject.getString("text");

            JSONObject user = jsonObject.getJSONObject("user");
            String userName = user.getString("name");
            int followersCount = user.getInt("followers_count");

            boolean isRT = Utils.getJSONObjectOrNull(jsonObject, "retweeted_status") != null;
            int retweetCount = jsonObject.getInt("retweet_count");

            JSONObject entities = jsonObject.getJSONObject("entities");
            String[] hashtags = Utils.toStringArray(entities.getJSONArray("hashtags"));

            context.write(new LongWritable(id),
                    new TweetWritable(id,
                            timestamp,
                            text,
                            userName,
                            followersCount,
                            isRT,
                            retweetCount,
                            hashtags
                    ));
        } catch (JSONException e) {
            System.out.println("Something went wrong while parsing");
            System.out.println(e.getMessage());
        }
    }
}
