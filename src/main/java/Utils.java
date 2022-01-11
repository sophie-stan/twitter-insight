/**
 * @original author
 * Sophie Stan & Deborah Perreira
 */

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

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

    public static String[] toStringArray(JSONArray array) {
        if (array == null)
            return null;

        String[] arr = new String[array.length()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = array.optString(i);
        }
        return arr;
    }
}
