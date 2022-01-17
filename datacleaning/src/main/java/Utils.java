import com.google.gson.GsonBuilder;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @original author
 * Sophie Stan & Deborah Pereira
 */

public final class Utils {

    public static JSONObject getJSONObjectOrNull(JSONObject jsonObject, String key) {
        if (jsonObject.has(key)) {
            try {
                return jsonObject.getJSONObject(key);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static JSONArray getJSONArrayOrNull(JSONObject jsonObject, String key) {
        if (jsonObject == null)
            return null;

        if (jsonObject.has(key)) {
            try {
                return jsonObject.getJSONArray(key);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static List<String> getHashtags(JSONArray hashtagsArray) {
        List<String> hashtags = new ArrayList<>();

        if (hashtagsArray == null)
            return hashtags;

        for (int i = 0; i < hashtagsArray.length(); i++) {
            try {
                hashtags.add(hashtagsArray.getJSONObject(i).getString("text"));
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        return hashtags;
    }
}
