package com.superbias.smp3.android;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Persistent, user-visible configuration for one independently managed SMP3 client. */
public final class Smp3Instance {
    public String id;
    public String name;
    public boolean enabled;
    public String localSocksHost;
    public String localSocksPort;
    public String serverEndpoint;
    /** Stored in the app-private sandbox and never copied into runtime logs. */
    public String smp3Password;
    public final List<CarrierEndpoint> carriers = new ArrayList<>();
    public long createdAt;
    public long updatedAt;

    public Smp3Instance() {
        this(UUID.randomUUID().toString(), "Default", true, "127.0.0.1", "18080",
                "127.0.0.1:24445", "", System.currentTimeMillis(), System.currentTimeMillis());
        carriers.add(new CarrierEndpoint(UUID.randomUUID().toString(), "Carrier A", "127.0.0.1", "20001"));
        carriers.add(new CarrierEndpoint(UUID.randomUUID().toString(), "Carrier B", "127.0.0.1", "20002"));
    }

    public Smp3Instance(String id, String name, boolean enabled, String localSocksHost,
                        String localSocksPort, String serverEndpoint, String smp3Password,
                        long createdAt, long updatedAt) {
        this.id = id == null || id.isEmpty() ? UUID.randomUUID().toString() : id;
        this.name = name == null || name.trim().isEmpty() ? "Default" : name;
        this.enabled = enabled;
        this.localSocksHost = localSocksHost == null ? "127.0.0.1" : localSocksHost;
        this.localSocksPort = localSocksPort == null ? "18080" : localSocksPort;
        this.serverEndpoint = serverEndpoint == null ? "127.0.0.1:24445" : serverEndpoint;
        this.smp3Password = smp3Password == null ? "" : smp3Password;
        this.createdAt = createdAt == 0 ? System.currentTimeMillis() : createdAt;
        this.updatedAt = updatedAt == 0 ? this.createdAt : updatedAt;
    }

    public static Smp3Instance fromLegacy(Smp3Config legacy) {
        Smp3Instance instance = new Smp3Instance(
                UUID.randomUUID().toString(), "Default", true, legacy.localHost, legacy.localPort,
                legacy.server, legacy.smp3Password, System.currentTimeMillis(), System.currentTimeMillis());
        instance.carriers.clear();
        instance.carriers.add(new CarrierEndpoint(UUID.randomUUID().toString(), "Carrier A",
                legacy.carrierAHost, legacy.carrierAPort));
        instance.carriers.add(new CarrierEndpoint(UUID.randomUUID().toString(), "Carrier B",
                legacy.carrierBHost, legacy.carrierBPort));
        return instance;
    }

    public static Smp3Instance defaultInstance(String name, int localPort, int carrierBasePort) {
        Smp3Instance instance = new Smp3Instance();
        instance.name = name == null || name.trim().isEmpty() ? "New Instance" : name;
        instance.localSocksPort = String.valueOf(localPort);
        instance.carriers.clear();
        instance.carriers.add(new CarrierEndpoint(UUID.randomUUID().toString(), "Carrier A", "127.0.0.1",
                String.valueOf(carrierBasePort)));
        instance.carriers.add(new CarrierEndpoint(UUID.randomUUID().toString(), "Carrier B", "127.0.0.1",
                String.valueOf(carrierBasePort + 1)));
        return instance;
    }

    public Smp3Instance copy() {
        Smp3Instance copy = new Smp3Instance(id, name, enabled, localSocksHost, localSocksPort,
                serverEndpoint, smp3Password, createdAt, updatedAt);
        for (CarrierEndpoint carrier : carriers) copy.carriers.add(carrier.copy());
        return copy;
    }

    public String localAddress() {
        return Smp3Config.hostPort(localSocksHost, localSocksPort);
    }

    public String validationError() {
        if (name == null || name.trim().isEmpty()) return "Instance name is required";
        if (!"127.0.0.1".equals(localSocksHost) && !"::1".equals(localSocksHost)) {
            return "Local SOCKS host must be 127.0.0.1 or ::1";
        }
        if (!Smp3Config.validPort(localSocksPort)) return "Local SOCKS port invalid";
        if (!Smp3Config.validEndpoint(serverEndpoint)) return "Invalid server endpoint (use host:port)";
        if (smp3Password == null || smp3Password.isEmpty()) return "SMP3 protocol password is required";
        if (carriers.size() != 2) return "This SMP3 runtime requires exactly 2 carrier endpoints";
        for (int i = 0; i < carriers.size(); i++) {
            CarrierEndpoint carrier = carriers.get(i);
            if (carrier == null || !Smp3Config.validHost(carrier.host) || !Smp3Config.validPort(carrier.port)) {
                return "Carrier " + (i + 1) + " SOCKS endpoint invalid";
            }
        }
        return null;
    }

    JSONObject toJson() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("name", name);
        json.put("enabled", enabled);
        json.put("local_socks_host", localSocksHost);
        json.put("local_socks_port", localSocksPort);
        json.put("server_endpoint", serverEndpoint);
        json.put("smp3_password", smp3Password);
        json.put("created_at", createdAt);
        json.put("updated_at", updatedAt);
        JSONArray carrierArray = new JSONArray();
        for (CarrierEndpoint carrier : carriers) carrierArray.put(carrier.toJson());
        json.put("carriers", carrierArray);
        return json;
    }

    static Smp3Instance fromJson(JSONObject json) {
        Smp3Instance instance = new Smp3Instance(
                json.optString("id", UUID.randomUUID().toString()),
                json.optString("name", "Default"),
                json.optBoolean("enabled", true),
                json.optString("local_socks_host", "127.0.0.1"),
                json.optString("local_socks_port", "18080"),
                json.optString("server_endpoint", "127.0.0.1:24445"),
                json.has("smp3_password") ? json.optString("smp3_password", "") : json.optString("password", ""),
                json.optLong("created_at", System.currentTimeMillis()),
                json.optLong("updated_at", System.currentTimeMillis()));
        instance.carriers.clear();
        JSONArray carrierArray = json.optJSONArray("carriers");
        if (carrierArray != null) {
            for (int i = 0; i < carrierArray.length(); i++) {
                JSONObject carrier = carrierArray.optJSONObject(i);
                if (carrier != null) instance.carriers.add(CarrierEndpoint.fromJson(carrier, i));
            }
        }
        if (instance.carriers.isEmpty()) {
            instance.carriers.add(new CarrierEndpoint(UUID.randomUUID().toString(), "Carrier A",
                    json.optString("carrier_a_host", "127.0.0.1"), json.optString("carrier_a_port", "20001")));
            instance.carriers.add(new CarrierEndpoint(UUID.randomUUID().toString(), "Carrier B",
                    json.optString("carrier_b_host", "127.0.0.1"), json.optString("carrier_b_port", "20002")));
        }
        return instance;
    }
}
