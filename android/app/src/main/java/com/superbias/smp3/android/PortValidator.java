package com.superbias.smp3.android;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;

/** Validates app-managed endpoint collisions before a configuration is saved. */
public final class PortValidator {
    private PortValidator() { }

    public static String validate(List<Smp3Instance> existing, Smp3Instance candidate) {
        Map<String, String> owners = new HashMap<>();
        for (Smp3Instance instance : existing) {
            if (instance == null || sameId(instance, candidate)) continue;
            add(owners, endpointKey(instance.localSocksHost, instance.localSocksPort),
                    instance.name + " (Local SOCKS)");
            for (int i = 0; i < instance.carriers.size(); i++) {
                CarrierEndpoint carrier = instance.carriers.get(i);
                if (carrier != null) add(owners, endpointKey(carrier.host, carrier.port),
                        instance.name + " (Carrier " + (i + 1) + ")");
            }
        }

        String conflict = check(owners, endpointKey(candidate.localSocksHost, candidate.localSocksPort),
                "Local SOCKS", candidate.localSocksPort);
        if (conflict != null) return conflict;
        owners.put(endpointKey(candidate.localSocksHost, candidate.localSocksPort), candidate.name + " (Local SOCKS)");
        for (int i = 0; i < candidate.carriers.size(); i++) {
            CarrierEndpoint carrier = candidate.carriers.get(i);
            if (carrier == null) continue;
            conflict = check(owners, endpointKey(carrier.host, carrier.port),
                    "Carrier " + (i + 1), carrier.port);
            if (conflict != null) return conflict;
            owners.put(endpointKey(carrier.host, carrier.port), candidate.name + " (Carrier " + (i + 1) + ")");
        }
        return null;
    }

    public static int recommendLocalPort(List<Smp3Instance> instances) {
        for (int port = 18080; port <= 18180; port++) {
            boolean used = false;
            for (Smp3Instance instance : instances) {
                if (instance != null && String.valueOf(port).equals(instance.localSocksPort)) {
                    used = true;
                    break;
                }
            }
            if (!used) return port;
        }
        return 18181;
    }

    public static int recommendCarrierBasePort(List<Smp3Instance> instances) {
        int base = 20001;
        while (containsPort(instances, base) || containsPort(instances, base + 1)) base += 10;
        return base;
    }

    private static boolean containsPort(List<Smp3Instance> instances, int port) {
        String value = String.valueOf(port);
        for (Smp3Instance instance : instances) {
            if (instance == null) continue;
            if (value.equals(instance.localSocksPort)) return true;
            for (CarrierEndpoint carrier : instance.carriers) {
                if (carrier != null && value.equals(carrier.port) && isLoopback(carrier.host)) return true;
            }
        }
        return false;
    }

    private static String check(Map<String, String> owners, String key, String endpoint, String port) {
        String owner = owners.get(key);
        return owner == null ? null : "Port " + port + " for " + endpoint + " is already used by \"" + owner + "\"";
    }

    private static void add(Map<String, String> owners, String key, String owner) {
        if (key != null && !owners.containsKey(key)) owners.put(key, owner);
    }

    private static boolean sameId(Smp3Instance left, Smp3Instance right) {
        return right != null && left.id != null && left.id.equals(right.id);
    }

    private static String endpointKey(String host, String port) {
        if (host == null || port == null) return null;
        if (isLoopback(host)) return "loopback:" + port;
        return host.trim().toLowerCase(Locale.ROOT) + ":" + port.trim();
    }

    private static boolean isLoopback(String host) {
        return "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host) || "::1".equals(host);
    }
}
