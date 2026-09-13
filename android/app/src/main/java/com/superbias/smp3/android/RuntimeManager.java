package com.superbias.smp3.android;

import android.content.Context;

import org.json.JSONException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Owns one isolated native process and bounded logs for every configured instance. */
public final class RuntimeManager {
    public enum RuntimeState { STOPPED, STARTING, RUNNING, SOCKS_READY, ERROR, STOPPING }

    public interface Listener {
        void onRuntimeChanged();
    }

    public static final class LogEntry {
        public final long timestamp;
        public final String instanceId;
        public final String level;
        public final String message;

        LogEntry(long timestamp, String instanceId, String level, String message) {
            this.timestamp = timestamp;
            this.instanceId = instanceId;
            this.level = level;
            this.message = message;
        }

        public String display() {
            String time = new SimpleDateFormat("HH:mm:ss", Locale.US).format(new Date(timestamp));
            return time + "  " + level + "  " + message;
        }
    }

    public static final class RuntimeSnapshot {
        public final String instanceId;
        public final String name;
        public final boolean enabled;
        public final String localAddress;
        public final String serverEndpoint;
        public final List<String> carrierAddresses;
        public final RuntimeState state;
        public final boolean processRunning;
        public final boolean socksReady;
        public final long processIdentity;
        public final int exitCode;
        public final String error;
        public final long startTime;
        public final List<LogEntry> logEntries;

        RuntimeSnapshot(String instanceId, String name, boolean enabled, String localAddress,
                        String serverEndpoint, List<String> carrierAddresses, RuntimeState state,
                        boolean processRunning, boolean socksReady, long processIdentity,
                        int exitCode, String error, long startTime, List<LogEntry> logEntries) {
            this.instanceId = instanceId;
            this.name = name;
            this.enabled = enabled;
            this.localAddress = localAddress;
            this.serverEndpoint = serverEndpoint;
            this.carrierAddresses = carrierAddresses;
            this.state = state;
            this.processRunning = processRunning;
            this.socksReady = socksReady;
            this.processIdentity = processIdentity;
            this.exitCode = exitCode;
            this.error = error;
            this.startTime = startTime;
            this.logEntries = logEntries;
        }

        static RuntimeSnapshot idle(Smp3Instance instance) {
            List<String> addresses = new ArrayList<>();
            for (CarrierEndpoint carrier : instance.carriers) addresses.add(carrier.address());
            return new RuntimeSnapshot(instance.id, instance.name, instance.enabled, instance.localAddress(),
                    instance.serverEndpoint, addresses, RuntimeState.STOPPED, false, false, -1,
                    Integer.MIN_VALUE, "", 0, new ArrayList<>());
        }
    }

    private static final int MAX_LOG_LINES = 1000;
    private static final Pattern SECRET = Pattern.compile(
            "(?i)(password|token|psk|private[-_ ]?key|uuid|secret)(\\s*[:=]\\s*)\\S+");

    private static final class InstanceRuntime {
        Smp3Instance config;
        Process process;
        RuntimeState state = RuntimeState.STOPPED;
        boolean socksReady;
        boolean stopRequested;
        long processIdentity = -1;
        long startTime;
        int exitCode = Integer.MIN_VALUE;
        String error = "";
        final ArrayDeque<LogEntry> logs = new ArrayDeque<>();

        InstanceRuntime(Smp3Instance config) {
            this.config = config.copy();
        }
    }

    private final Context context;
    private final Listener listener;
    private final Object lock = new Object();
    private final Map<String, InstanceRuntime> runtimes = new LinkedHashMap<>();
    private final ExecutorService operations = Executors.newSingleThreadExecutor();
    private final ExecutorService ioPool = Executors.newCachedThreadPool();
    private long nextProcessIdentity = 1;
    private boolean destroyed;

    public RuntimeManager(Context context, Listener listener) {
        this.context = context.getApplicationContext();
        this.listener = listener;
        reloadInstances();
    }

    public void reloadInstances() {
        List<Smp3Instance> instances = InstanceStore.load(context);
        synchronized (lock) {
            Map<String, Smp3Instance> incoming = new LinkedHashMap<>();
            for (Smp3Instance instance : instances) {
                incoming.put(instance.id, instance);
                InstanceRuntime runtime = runtimes.get(instance.id);
                if (runtime == null) {
                    runtimes.put(instance.id, new InstanceRuntime(instance));
                } else if (runtime.process == null && runtime.state != RuntimeState.STARTING
                        && runtime.state != RuntimeState.STOPPING) {
                    runtime.config = instance.copy();
                }
            }
            List<String> removed = new ArrayList<>();
            for (Map.Entry<String, InstanceRuntime> entry : runtimes.entrySet()) {
                if (!incoming.containsKey(entry.getKey()) && entry.getValue().process == null) removed.add(entry.getKey());
            }
            for (String id : removed) runtimes.remove(id);
        }
        notifyChanged();
    }

    public List<RuntimeSnapshot> snapshots() {
        List<RuntimeSnapshot> result = new ArrayList<>();
        synchronized (lock) {
            for (InstanceRuntime runtime : runtimes.values()) result.add(snapshotOf(runtime));
        }
        return result;
    }

    public RuntimeSnapshot snapshot(String instanceId) {
        synchronized (lock) {
            InstanceRuntime runtime = runtimes.get(instanceId);
            return runtime == null ? null : snapshotOf(runtime);
        }
    }

    public boolean hasActiveInstances() {
        synchronized (lock) {
            for (InstanceRuntime runtime : runtimes.values()) {
                if (runtime.process != null || runtime.state == RuntimeState.STARTING
                        || runtime.state == RuntimeState.STOPPING) return true;
            }
            return false;
        }
    }

    public void startInstance(final String instanceId) {
        submit(() -> startInstanceInternal(instanceId));
    }

    public void stopInstance(final String instanceId) {
        submit(() -> stopInstanceInternal(instanceId));
    }

    public void restartInstance(final String instanceId) {
        submit(() -> {
            stopInstanceInternal(instanceId);
            startInstanceInternal(instanceId);
        });
    }

    public void startAll() {
        submit(() -> {
            List<String> ids = new ArrayList<>();
            synchronized (lock) {
                for (InstanceRuntime runtime : runtimes.values()) if (runtime.config.enabled) ids.add(runtime.config.id);
            }
            for (String id : ids) startInstanceInternal(id);
        });
    }

    public void stopAll() {
        submit(() -> {
            List<String> ids = new ArrayList<>();
            synchronized (lock) {
                ids.addAll(runtimes.keySet());
            }
            for (String id : ids) stopInstanceInternal(id);
        });
    }

    public void clearLogs(String instanceId) {
        synchronized (lock) {
            InstanceRuntime runtime = runtimes.get(instanceId);
            if (runtime != null) runtime.logs.clear();
        }
        notifyChanged();
    }

    public void clearAllLogs() {
        synchronized (lock) {
            for (InstanceRuntime runtime : runtimes.values()) runtime.logs.clear();
        }
        notifyChanged();
    }

    /** Synchronously tears down only managed children for force-stop/service destruction cleanup. */
    public void shutdown() {
        synchronized (lock) {
            destroyed = true;
        }
        List<InstanceRuntime> current;
        synchronized (lock) {
            current = new ArrayList<>(runtimes.values());
        }
        for (InstanceRuntime runtime : current) stopProcessNow(runtime);
        operations.shutdownNow();
        ioPool.shutdownNow();
    }

    private void submit(Runnable action) {
        try {
            operations.execute(action);
        } catch (RuntimeException ignored) {
            // The service is shutting down; no new child may be started.
        }
    }

    private void startInstanceInternal(String instanceId) {
        Smp3Instance config;
        InstanceRuntime runtime;
        synchronized (lock) {
            runtime = runtimes.get(instanceId);
            if (runtime == null || destroyed || runtime.process != null || runtime.state == RuntimeState.STARTING
                    || runtime.state == RuntimeState.RUNNING || runtime.state == RuntimeState.SOCKS_READY) return;
            config = runtime.config.copy();
        }
        String validation = config.validationError();
        if (validation == null) validation = PortValidator.validate(InstanceStore.load(context), config);
        if (validation != null) {
            setError(runtime, validation);
            return;
        }

        synchronized (lock) {
            if (destroyed || runtime.process != null) return;
            runtime.state = RuntimeState.STARTING;
            runtime.socksReady = false;
            runtime.exitCode = Integer.MIN_VALUE;
            runtime.error = "";
            runtime.stopRequested = false;
            runtime.startTime = System.currentTimeMillis();
        }
        appendLog(runtime, "INFO", "Starting SMP3 instance");
        notifyChanged();

        File configFile = InstanceStore.configFile(context, config.id);
        try {
            writeConfig(configFile, config);
        } catch (IOException | JSONException error) {
            setError(runtime, "Unable to write instance config");
            return;
        }
        File executable = new File(context.getApplicationInfo().nativeLibraryDir, "libsmp3-client.so");
        if (!executable.isFile()) {
            setError(runtime, "Packaged SMP3 executable is missing");
            return;
        }
        if (!executable.canExecute() && !executable.setExecutable(true, false)) {
            setError(runtime, "Packaged SMP3 executable is not executable");
            return;
        }
        try {
            Process started = new ProcessBuilder(executable.getAbsolutePath(), "-c", configFile.getAbsolutePath())
                    .redirectErrorStream(false).start();
            synchronized (lock) {
                if (destroyed) {
                    started.destroy();
                    return;
                }
                runtime.process = started;
                runtime.processIdentity = nextProcessIdentity++;
                runtime.state = RuntimeState.RUNNING;
            }
            appendLog(runtime, "INFO", "SMP3 process started; managed-id=" + runtime.processIdentity);
            ioPool.execute(() -> pump(runtime, "stdout", started.getInputStream()));
            ioPool.execute(() -> pump(runtime, "stderr", started.getErrorStream()));
            ioPool.execute(() -> waitForChild(runtime, started));
            ioPool.execute(() -> monitorReadiness(runtime, started));
            notifyChanged();
        } catch (IOException error) {
            setError(runtime, "Unable to start SMP3 process");
        }
    }

    private void stopInstanceInternal(String instanceId) {
        InstanceRuntime runtime;
        Process process;
        synchronized (lock) {
            runtime = runtimes.get(instanceId);
            if (runtime == null) return;
            if (runtime.process == null) {
                if (runtime.state == RuntimeState.STARTING) {
                    runtime.stopRequested = true;
                    runtime.state = RuntimeState.STOPPING;
                    notifyChanged();
                }
                return;
            }
            process = runtime.process;
            runtime.stopRequested = true;
            runtime.state = RuntimeState.STOPPING;
            runtime.socksReady = false;
        }
        appendLog(runtime, "INFO", "Stopping SMP3 process");
        notifyChanged();
        stopProcess(process, runtime);
        synchronized (lock) {
            if (runtime.process == process) {
                runtime.process = null;
                runtime.processIdentity = -1;
                runtime.state = RuntimeState.STOPPED;
                runtime.socksReady = false;
            }
        }
        notifyChanged();
    }

    private void stopProcessNow(InstanceRuntime runtime) {
        Process process;
        synchronized (lock) {
            process = runtime.process;
            if (process == null) continueStopping(runtime);
            else {
                runtime.stopRequested = true;
                runtime.state = RuntimeState.STOPPING;
            }
        }
        if (process != null) stopProcess(process, runtime);
        synchronized (lock) {
            if (runtime.process == process) {
                runtime.process = null;
                runtime.processIdentity = -1;
                runtime.socksReady = false;
                runtime.state = RuntimeState.STOPPED;
            }
        }
    }

    private void continueStopping(InstanceRuntime runtime) {
        runtime.processIdentity = -1;
        runtime.socksReady = false;
        runtime.state = RuntimeState.STOPPED;
    }

    private void stopProcess(Process process, InstanceRuntime runtime) {
        process.destroy();
        try {
            if (!process.waitFor(3, TimeUnit.SECONDS)) {
                appendLog(runtime, "WARN", "Process did not exit gracefully; forcing only this instance");
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void waitForChild(InstanceRuntime runtime, Process process) {
        try {
            int exitCode = process.waitFor();
            boolean intentional;
            synchronized (lock) {
                if (runtime.process != process) return;
                intentional = runtime.stopRequested;
                runtime.process = null;
                runtime.processIdentity = -1;
                runtime.socksReady = false;
                runtime.exitCode = exitCode;
                runtime.state = intentional || exitCode == 0 ? RuntimeState.STOPPED : RuntimeState.ERROR;
                runtime.error = intentional || exitCode == 0 ? "" : "SMP3 process exited unexpectedly (code " + exitCode + ")";
            }
            appendLog(runtime, intentional || exitCode == 0 ? "INFO" : "ERROR",
                    "SMP3 process exited; code=" + exitCode);
            notifyChanged();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void pump(InstanceRuntime runtime, String streamName, InputStream stream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) appendLog(runtime, "INFO", streamName + ": " + redact(line));
        } catch (IOException ignored) {
            appendLog(runtime, "INFO", streamName + " closed");
        }
    }

    private void monitorReadiness(InstanceRuntime runtime, Process process) {
        while (!Thread.currentThread().isInterrupted()) {
            String address;
            synchronized (lock) {
                if (destroyed || runtime.process != process || !process.isAlive()) return;
                address = runtime.config.localAddress();
            }
            boolean ready = canConnect(address);
            synchronized (lock) {
                if (runtime.process != process || runtime.state == RuntimeState.STOPPING) return;
                runtime.socksReady = ready;
                runtime.state = ready ? RuntimeState.SOCKS_READY : RuntimeState.RUNNING;
            }
            notifyChanged();
            try {
                Thread.sleep(250);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean canConnect(String address) {
        int separator = address.lastIndexOf(':');
        if (separator <= 0) return false;
        String host = address.substring(0, separator);
        if (host.startsWith("[") && host.endsWith("]")) host = host.substring(1, host.length() - 1);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, Integer.parseInt(address.substring(separator + 1))), 200);
            return true;
        } catch (IOException | NumberFormatException ignored) {
            return false;
        }
    }

    private void writeConfig(File file, Smp3Instance instance) throws IOException, JSONException {
        byte[] data = Smp3Config.fromInstance(instance).toClientJson().toString(2).getBytes(StandardCharsets.UTF_8);
        try (FileOutputStream output = new FileOutputStream(file, false)) {
            output.write(data);
            output.getFD().sync();
        }
    }

    private void setError(InstanceRuntime runtime, String error) {
        synchronized (lock) {
            runtime.state = RuntimeState.ERROR;
            runtime.socksReady = false;
            runtime.error = error;
            runtime.exitCode = Integer.MIN_VALUE;
        }
        appendLog(runtime, "ERROR", error);
        notifyChanged();
    }

    private void appendLog(InstanceRuntime runtime, String level, String message) {
        String safe = redact(message);
        synchronized (lock) {
            runtime.logs.addLast(new LogEntry(System.currentTimeMillis(), runtime.config.id, level, safe));
            while (runtime.logs.size() > MAX_LOG_LINES) runtime.logs.removeFirst();
        }
        notifyChanged();
    }

    private RuntimeSnapshot snapshotOf(InstanceRuntime runtime) {
        List<String> carriers = new ArrayList<>();
        for (CarrierEndpoint carrier : runtime.config.carriers) carriers.add(carrier.address());
        return new RuntimeSnapshot(runtime.config.id, runtime.config.name, runtime.config.enabled,
                runtime.config.localAddress(), runtime.config.serverEndpoint, carriers, runtime.state,
                runtime.process != null && runtime.process.isAlive(), runtime.socksReady,
                runtime.process == null ? -1 : runtime.processIdentity, runtime.exitCode, runtime.error,
                runtime.startTime, new ArrayList<>(runtime.logs));
    }

    private void notifyChanged() {
        if (listener != null) listener.onRuntimeChanged();
    }

    private static String redact(String line) {
        Matcher matcher = SECRET.matcher(line == null ? "" : line);
        return matcher.replaceAll("$1$2<redacted>");
    }
}
