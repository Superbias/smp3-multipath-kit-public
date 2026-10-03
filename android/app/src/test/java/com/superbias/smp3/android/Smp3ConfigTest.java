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

}
