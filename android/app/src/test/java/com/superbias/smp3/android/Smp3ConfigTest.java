package com.superbias.smp3.android;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class Smp3ConfigTest {
    @Test
    public void defaultsUseLoopbackAndTwoDistinctCarriers() {
        Smp3Config config = new Smp3Config();
        config.smp3Password = "protocol-only-test-value";
        assertNull(config.validationError());
        assertEquals("127.0.0.1:18080", config.localAddress());
        assertEquals("127.0.0.1:20001", config.carrierAAddress());
        assertEquals("127.0.0.1:20002", config.carrierBAddress());
    }

    @Test
    public void invalidLocalExposureAndPortsAreRejected() {
        Smp3Config config = new Smp3Config();
        config.smp3Password = "protocol-only-test-value";
        config.localHost = "0.0.0.0";
        assertEquals("Local SOCKS host must be 127.0.0.1 or ::1", config.validationError());

        config.localHost = "127.0.0.1";
        config.localPort = "0";
        assertEquals("Local port invalid", config.validationError());
    }

    @Test
    public void instanceConversionPreservesBothRoutes() {
        Smp3Instance instance = Smp3Instance.defaultInstance("JP", 18080, 20001);
        instance.serverEndpoint = "10.66.66.1:24445";
        instance.smp3Password = "protocol-only-test-value";
        Smp3Config config = Smp3Config.fromInstance(instance);
        assertEquals("127.0.0.1:20001", config.carrierAAddress());
        assertEquals("127.0.0.1:20002", config.carrierBAddress());
        assertEquals("10.66.66.1:24445", config.server);
    }

    @Test
    public void legacyConfigMapsToStableInstance() {
        Smp3Config legacy = new Smp3Config();
        legacy.smp3Password = "protocol-only-test-value";
        legacy.server = "10.66.66.1:24445";
        Smp3Instance instance = Smp3Instance.fromLegacy(legacy);
        assertEquals("127.0.0.1:18080", instance.localAddress());
        assertEquals("10.66.66.1:24445", instance.serverEndpoint);
        assertEquals(2, instance.carriers.size());
        assertEquals("127.0.0.1:20002", instance.carriers.get(1).address());
    }
}
