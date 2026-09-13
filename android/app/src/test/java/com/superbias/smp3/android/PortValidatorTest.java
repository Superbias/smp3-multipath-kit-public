package com.superbias.smp3.android;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class PortValidatorTest {
    @Test
    public void distinctInstancesCanUseRecommendedPorts() {
        Smp3Instance first = Smp3Instance.defaultInstance("JP", 18080, 20001);
        Smp3Instance second = Smp3Instance.defaultInstance("US", 18081, 20011);
        assertNull(PortValidator.validate(Arrays.asList(first), second));
        List<Smp3Instance> all = new ArrayList<>();
        all.add(first);
        assertEquals(18081, PortValidator.recommendLocalPort(all));
        assertEquals(20011, PortValidator.recommendCarrierBasePort(all));
    }

    @Test
    public void localPortCollisionIsReportedBeforeStart() {
        Smp3Instance first = Smp3Instance.defaultInstance("JP", 18080, 20001);
        Smp3Instance second = Smp3Instance.defaultInstance("US", 18080, 20011);
        String error = PortValidator.validate(Arrays.asList(first), second);
        assertTrue(error.contains("18080"));
        assertTrue(error.contains("JP"));
    }

    @Test
    public void localAndCarrierCollisionIsReported() {
        Smp3Instance first = Smp3Instance.defaultInstance("JP", 18080, 20001);
        Smp3Instance second = Smp3Instance.defaultInstance("US", 18081, 20001);
        String error = PortValidator.validate(Arrays.asList(first), second);
        assertTrue(error.contains("20001"));
        assertTrue(error.contains("JP"));
    }

    @Test
    public void localAndCarrierCollisionWithinCandidateIsReported() {
        Smp3Instance instance = Smp3Instance.defaultInstance("JP", 18080, 20001);
        instance.carriers.get(0).port = "18080";
        String error = PortValidator.validate(new ArrayList<>(), instance);
        assertTrue(error.contains("18080"));
    }
}
