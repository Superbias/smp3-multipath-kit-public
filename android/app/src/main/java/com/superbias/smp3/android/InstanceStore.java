package com.superbias.smp3.android;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Small atomic JSON store for instance metadata and private per-instance runtime files. */
public final class InstanceStore {
    private static final String METADATA_FILE = "instances.json";
    private static final String INSTANCE_DIR = "instances";
    private static final String PREFS = "smp3_config";
    private static final String MIGRATED = "instances_migrated";

    private InstanceStore() { }

    public static synchronized List<Smp3Instance> load(Context context) {
        File metadata = new File(context.getFilesDir(), METADATA_FILE);
        if (!metadata.isFile()) {
            List<Smp3Instance> migrated = new ArrayList<>();
            if (hasLegacyConfiguration(context)) migrated.add(Smp3Instance.fromLegacy(Smp3Config.load(context)));
            else migrated.add(new Smp3Instance());
            try {
                save(context, migrated);
                context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().putBoolean(MIGRATED, true).apply();
            } catch (IOException | JSONException ignored) {
                // The in-memory default remains usable; the next successful save will persist it.
            }
            return migrated;
        }
        try {
            String text = java.nio.file.Files.readAllLines(metadata.toPath(), StandardCharsets.UTF_8)
                    .stream().reduce("", (left, right) -> left + right);
            JSONArray array = new JSONObject(text).optJSONArray("instances");
            List<Smp3Instance> result = new ArrayList<>();
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    JSONObject item = array.optJSONObject(i);
                    if (item != null) result.add(Smp3Instance.fromJson(item));
                }
            }
            return result;
        } catch (IOException | JSONException error) {
            return new ArrayList<>();
        }
    }

    public static synchronized void save(Context context, List<Smp3Instance> instances)
            throws IOException, JSONException {
        File metadata = new File(context.getFilesDir(), METADATA_FILE);
        File temporary = new File(context.getFilesDir(), METADATA_FILE + ".tmp");
        JSONObject root = new JSONObject();
        root.put("schema_version", 1);
        JSONArray array = new JSONArray();
        for (Smp3Instance instance : instances) {
            instance.updatedAt = System.currentTimeMillis();
            array.put(instance.toJson());
        }
        root.put("instances", array);
        writeBytes(temporary, root.toString(2).getBytes(StandardCharsets.UTF_8));
        if (!temporary.renameTo(metadata)) {
            if (metadata.exists() && !metadata.delete()) throw new IOException("Unable to replace instance metadata");
            if (!temporary.renameTo(metadata)) throw new IOException("Unable to store instance metadata");
        }
    }

    public static File configFile(Context context, String instanceId) {
        File directory = new File(context.getFilesDir(), INSTANCE_DIR);
        if (!directory.isDirectory() && !directory.mkdirs()) {
            // The subsequent write reports the useful error if the directory cannot be created.
        }
        return new File(directory, safeId(instanceId) + ".json");
    }

    public static void deleteConfig(Context context, String instanceId) {
        File file = configFile(context, instanceId);
        if (file.isFile() && !file.delete()) {
            // A stale private config is harmless and will be overwritten on the next start.
        }
    }

    private static boolean hasLegacyConfiguration(Context context) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
        return prefs.contains("server") || prefs.contains("local_port") || prefs.contains("smp3_password")
                || prefs.getBoolean(MIGRATED, false);
    }

    private static void writeBytes(File file, byte[] bytes) throws IOException {
        try (FileOutputStream output = new FileOutputStream(file, false)) {
            output.write(bytes);
            output.getFD().sync();
        }
    }

    private static String safeId(String value) {
        if (value == null || value.isEmpty()) return "unknown";
        return value.replaceAll("[^A-Za-z0-9_-]", "_");
    }
}
