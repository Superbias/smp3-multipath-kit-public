package com.superbias.smp3.android;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.UUID;

/** A standard SOCKS5 endpoint supplied by an external proxy core. */
public final class CarrierEndpoint {
    public String id;
    public String name;
    public String host;
    public String port;

    public CarrierEndpoint() {
        this(UUID.randomUUID().toString(), "Carrier", "127.0.0.1", "20001");
    }

    public CarrierEndpoint(String id, String name, String host, String port) {
        this.id = id == null || id.isEmpty() ? UUID.randomUUID().toString() : id;
        this.name = name == null || name.isEmpty() ? "Carrier" : name;
        this.host = host == null ? "127.0.0.1" : host;
        this.port = port == null ? "" : port;
    }

    public CarrierEndpoint copy() {
        return new CarrierEndpoint(id, name, host, port);
    }

    public String address() {
        return Smp3Config.hostPort(host, port);
    }

    JSONObject toJson() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("name", name);
        json.put("host", host);
        json.put("port", port);
        return json;
    }

    static CarrierEndpoint fromJson(JSONObject json, int index) {
        String fallbackName = "Carrier " + (index + 1);
        return new CarrierEndpoint(
                json.optString("id", UUID.randomUUID().toString()),
                json.optString("name", fallbackName),
                json.optString("host", "127.0.0.1"),
                json.optString("port", index == 0 ? "20001" : "20002"));
    }
}
