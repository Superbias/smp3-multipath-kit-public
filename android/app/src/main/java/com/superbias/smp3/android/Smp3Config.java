package com.superbias.smp3.android;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONException;
import org.json.JSONObject;

/** The Android shell owns only SMP3 settings, not external proxy-core nodes. */
public final class Smp3Config {
    private static final String PREFS = "smp3_config";

    public String localHost = "127.0.0.1";
    public String localPort = "18080";
    public String server = "127.0.0.1:24445";
    public String carrierAHost = "127.0.0.1";
    public String carrierAPort = "20001";
    public String carrierBHost = "127.0.0.1";
    public String carrierBPort = "20002";
    /** This is the SMP3 protocol password, not a proxy-node credential. */
    public String smp3Password = "";

    public static Smp3Config load(Context context) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
        Smp3Config config = new Smp3Config();
        config.localHost = prefs.getString("local_host", config.localHost);
        config.localPort = prefs.getString("local_port", config.localPort);
        config.server = prefs.getString("server", config.server);
        config.carrierAHost = prefs.getString("carrier_a_host", config.carrierAHost);
        config.carrierAPort = prefs.getString("carrier_a_port", config.carrierAPort);
        config.carrierBHost = prefs.getString("carrier_b_host", config.carrierBHost);
        config.carrierBPort = prefs.getString("carrier_b_port", config.carrierBPort);
        config.smp3Password = prefs.getString("smp3_password", config.smp3Password);
        return config;
    }

    public void save(Context context) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit()
                .putString("local_host", localHost)
                .putString("local_port", localPort)
                .putString("server", server)
                .putString("carrier_a_host", carrierAHost)
                .putString("carrier_a_port", carrierAPort)
                .putString("carrier_b_host", carrierBHost)
                .putString("carrier_b_port", carrierBPort)
                .putString("smp3_password", smp3Password)
                .apply();
    }

    public String localAddress() {
        return hostPort(localHost, localPort);
    }

    public String carrierAAddress() {
        return hostPort(carrierAHost, carrierAPort);
    }

    public String carrierBAddress() {
        return hostPort(carrierBHost, carrierBPort);
    }

    public String validationError() {
        if (!"127.0.0.1".equals(localHost) && !"::1".equals(localHost)) {
            return "Local SOCKS host must be 127.0.0.1 or ::1";
        }
        if (!validPort(localPort)) {
            return "Local port invalid";
        }
        if (!validEndpoint(server)) {
            return "Invalid server endpoint (use host:port)";
        }
        if (!validHost(carrierAHost) || !validPort(carrierAPort)) {
            return "Carrier A SOCKS endpoint invalid";
        }
        if (!validHost(carrierBHost) || !validPort(carrierBPort)) {
            return "Carrier B SOCKS endpoint invalid";
        }
        if (smp3Password == null || smp3Password.isEmpty()) {
            return "SMP3 protocol password is required";
        }
        return null;
    }

    /** Creates the minimal JSON accepted by the existing standalone client. */
    public JSONObject toClientJson() throws JSONException {
        JSONObject root = new JSONObject();
        root.put("listen", localAddress());

        JSONObject upstream = new JSONObject();
        upstream.put("address", carrierAAddress());
        upstream.put("connect_timeout", "10s");
        JSONObject leg0 = new JSONObject();
        leg0.put("address", carrierAAddress());
        leg0.put("connect_timeout", "10s");
        JSONObject leg1 = new JSONObject();
        leg1.put("address", carrierBAddress());
        leg1.put("connect_timeout", "10s");
        upstream.put("leg0", leg0);
        upstream.put("leg1", leg1);
        root.put("upstream_socks", upstream);

        JSONObject routes = new JSONObject();
        routes.put("leg0", server);
        routes.put("leg1", server);
        JSONObject smp3 = new JSONObject();
        smp3.put("password", smp3Password);
        smp3.put("routes", routes);
        root.put("smp3", smp3);
        return root;
    }

    public static String hostPort(String host, String port) {
        if (host != null && host.indexOf(':') >= 0 && !host.startsWith("[")) {
            return "[" + host + "]:" + port;
        }
        return host + ":" + port;
    }

    private static boolean validHost(String value) {
        return value != null && !value.trim().isEmpty() && !value.contains(" ");
    }

    private static boolean validPort(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        try {
            int port = Integer.parseInt(value);
            return port >= 1 && port <= 65535;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private static boolean validEndpoint(String value) {
        if (value == null || value.trim().isEmpty() || value.contains(" ")) {
            return false;
        }
        String endpoint = value.trim();
        int portStart;
        String host;
        if (endpoint.startsWith("[")) {
            int close = endpoint.indexOf(']');
            if (close < 2 || close + 1 >= endpoint.length() || endpoint.charAt(close + 1) != ':') {
                return false;
            }
            host = endpoint.substring(1, close);
            portStart = close + 2;
        } else {
            portStart = endpoint.lastIndexOf(':') + 1;
            if (portStart <= 0 || endpoint.substring(0, portStart - 1).contains(":")) {
                return false;
            }
            host = endpoint.substring(0, portStart - 1);
        }
        return validHost(host) && validPort(endpoint.substring(portStart));
    }
}
