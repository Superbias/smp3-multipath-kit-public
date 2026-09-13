package com.superbias.smp3.android;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Test;

public class Smp3InstanceTest {
    @Test
    public void copyChangesDoNotMutateOriginal() {
        Smp3Instance original = new Smp3Instance();
        Smp3Instance copy = original.copy();
        copy.name = "Changed";
        copy.carriers.get(0).port = "20101";
        assertNotEquals(original.name, copy.name);
        assertEquals("20001", original.carriers.get(0).port);
        assertEquals(original.id, copy.id);
    }

    @Test
    public void runtimeRejectsUnsupportedCarrierCount() {
        Smp3Instance instance = new Smp3Instance();
        instance.smp3Password = "test-password";
        instance.carriers.remove(1);
        assertEquals("This SMP3 runtime requires exactly 2 carrier endpoints", instance.validationError());
    }
}
