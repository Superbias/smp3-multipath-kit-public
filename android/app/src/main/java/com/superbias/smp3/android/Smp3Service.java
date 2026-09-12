package com.superbias.smp3.android;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.os.Binder;
import android.os.Build;
import android.os.IBinder;

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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Supervises the unmodified SMP3 executable; it never processes SMP3 packets. */
public final class Smp3Service extends Service {
    public static final String ACTION_PREPARE = "com.superbias.smp3.android.PREPARE";
    public static final String ACTION_STOP = "com.superbias.smp3.android.STOP";

    private static final String CHANNEL_ID = "smp3-runtime";
    private static final int NOTIFICATION_ID = 1901;
    private static final int MAX_LOG_LINES = 200;
    private static final Pattern SECRET = Pattern.compile(
            "(?i)(password|token|psk|private[-_ ]?key|uuid|secret)(\\s*[:=]\\s*)\\S+");

    private final Object stateLock = new Object();
    private final ArrayDeque<String> logLines = new ArrayDeque<>();
    private final ExecutorService ioPool = Executors.newCachedThreadPool();
    private final IBinder binder = new LocalBinder();
    private Process child;
    private long nextProcessIdentity = 1;
    private long childIdentity = -1;
    private String localAddress = "127.0.0.1:18080";
    private boolean socksReady;
    private int lastExitCode = Integer.MIN_VALUE;
    private String lastError = "";
    private boolean destroyed;

    public final class LocalBinder extends Binder {
        public Smp3Service service() {
            return Smp3Service.this;
        }
    }

    public static final class RuntimeSnapshot {
        public final boolean processRunning;
        public final boolean socksReady;
        public final long processIdentity;
        public final int exitCode;
        public final String error;
        public final String localAddress;
        public final String logs;

        RuntimeSnapshot(boolean processRunning, boolean socksReady, long processIdentity,
                        int exitCode, String error, String localAddress, String logs) {
            this.processRunning = processRunning;
            this.socksReady = socksReady;
            this.processIdentity = processIdentity;
            this.exitCode = exitCode;
            this.error = error;
            this.localAddress = localAddress;
            this.logs = logs;
        }
    }

    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannel();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        String action = intent == null ? ACTION_PREPARE : intent.getAction();
        if (ACTION_STOP.equals(action)) {
            stopRuntime();
            stopForeground(STOP_FOREGROUND_REMOVE);
            stopSelf(startId);
            return START_NOT_STICKY;
        }
        ensureForeground();
        return START_NOT_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return binder;
    }

    public String startRuntime(Smp3Config config) {
        String validation = config.validationError();
        if (validation != null) {
            return validation;
        }
        ensureForeground();
        synchronized (stateLock) {
            if (child != null && child.isAlive()) {
                return "SMP3 is already running";
            }
            child = null;
            lastExitCode = Integer.MIN_VALUE;
            lastError = "";
            localAddress = config.localAddress();
            socksReady = false;
        }

        File configFile = new File(getFilesDir(), "smp3-client.json");
        try {
            writeConfig(configFile, config);
        } catch (IOException | JSONException error) {
            setError("Unable to write SMP3 config: " + error.getMessage());
            return "Unable to write SMP3 config";
        }

        File executable = new File(getApplicationInfo().nativeLibraryDir, "libsmp3-client.so");
        if (!executable.isFile()) {
            setError("Packaged SMP3 executable is missing");
            return "Packaged SMP3 executable is missing";
        }
        if (!executable.canExecute() && !executable.setExecutable(true, false)) {
            setError("Packaged SMP3 executable is not executable");
            return "Packaged SMP3 executable is not executable";
        }

        try {
            Process started = new ProcessBuilder(executable.getAbsolutePath(), "-c", configFile.getAbsolutePath())
                    .redirectErrorStream(false)
                    .start();
            long managedIdentity;
            synchronized (stateLock) {
                if (destroyed) {
                    started.destroy();
                    return "SMP3 service is stopping";
                }
                child = started;
                childIdentity = nextProcessIdentity++;
                managedIdentity = childIdentity;
            }
            appendLog("SMP3 child started; managed-id=" + managedIdentity);
            ioPool.execute(() -> pump("stdout", started.getInputStream()));
            ioPool.execute(() -> pump("stderr", started.getErrorStream()));
            ioPool.execute(() -> waitForChild(started));
            ioPool.execute(() -> monitorReadiness(started));
            updateNotification();
            return null;
        } catch (IOException error) {
            setError("Unable to start SMP3: " + error.getMessage());
            return "Unable to start SMP3";
        }
    }

    public void stopRuntime() {
        Process current;
        synchronized (stateLock) {
            current = child;
        }
        if (current == null) {
            return;
        }
        appendLog("Stopping SMP3 child");
        current.destroy();
        try {
            if (!current.waitFor(3, TimeUnit.SECONDS)) {
                appendLog("SMP3 did not exit gracefully; forcing only the managed process");
                current.destroyForcibly();
                current.waitFor(2, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        synchronized (stateLock) {
            if (child == current) {
                child = null;
                childIdentity = -1;
                socksReady = false;
            }
        }
        updateNotification();
    }

    public void clearLogs() {
        synchronized (stateLock) {
            logLines.clear();
        }
    }

    public RuntimeSnapshot snapshot() {
        boolean running;
        long identity = -1;
        int exitCode;
        String error;
        String address;
        String logs;
        synchronized (stateLock) {
            running = child != null && child.isAlive();
            if (running) {
                identity = childIdentity;
            }
            exitCode = lastExitCode;
            error = lastError;
            address = localAddress;
            logs = joinLogs();
        }
        boolean ready;
        synchronized (stateLock) {
            ready = running && socksReady;
        }
        return new RuntimeSnapshot(running, ready, identity, exitCode, error, address, logs);
    }

    @Override
    public void onDestroy() {
        destroyed = true;
        stopRuntime();
        ioPool.shutdownNow();
        super.onDestroy();
    }

    private void waitForChild(Process process) {
        try {
            int exitCode = process.waitFor();
            synchronized (stateLock) {
                if (child == process) {
                    child = null;
                    childIdentity = -1;
                    socksReady = false;
                    lastExitCode = exitCode;
                }
            }
            appendLog("SMP3 child exited; code=" + exitCode);
            updateNotification();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void pump(String streamName, InputStream stream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                appendLog(streamName + ": " + redact(line));
            }
        } catch (IOException error) {
            appendLog(streamName + " closed");
        }
    }

    private void monitorReadiness(Process process) {
        while (!Thread.currentThread().isInterrupted()) {
            String address;
            synchronized (stateLock) {
                if (destroyed || child != process || !process.isAlive()) {
                    return;
                }
                address = localAddress;
            }

            boolean ready = canConnect(address);
            synchronized (stateLock) {
                if (destroyed || child != process || !process.isAlive()) {
                    return;
                }
                socksReady = ready;
            }

            try {
                Thread.sleep(250);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void writeConfig(File file, Smp3Config config) throws IOException, JSONException {
        byte[] data = config.toClientJson().toString(2).getBytes(StandardCharsets.UTF_8);
        try (FileOutputStream output = new FileOutputStream(file, false)) {
            output.write(data);
        }
    }

    private boolean canConnect(String address) {
        int separator = address.lastIndexOf(':');
        if (separator <= 0) {
            return false;
        }
        String host = address.substring(0, separator);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, Integer.parseInt(address.substring(separator + 1))), 200);
            return true;
        } catch (IOException | NumberFormatException ignored) {
            return false;
        }
    }

    private void ensureForeground() {
        startForeground(NOTIFICATION_ID, buildNotification());
    }

    private void updateNotification() {
        NotificationManager manager = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (manager != null) {
            manager.notify(NOTIFICATION_ID, buildNotification());
        }
    }

    private Notification buildNotification() {
        RuntimeSnapshot state;
        synchronized (stateLock) {
            boolean running = child != null && child.isAlive();
            state = new RuntimeSnapshot(running, false, running ? childIdentity : -1,
                    lastExitCode, lastError, localAddress, "");
        }
        String title = state.processRunning ? "SMP3 Running" : "SMP3 Stopped";
        String text = state.processRunning ? "Local SOCKS: " + state.localAddress + " · Paths: 2" : "Ready to start";
        Intent launch = new Intent(this, MainActivity.class);
        int flags = PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE;
        PendingIntent pending = PendingIntent.getActivity(this, 0, launch, flags);
        return new Notification.Builder(this, CHANNEL_ID)
                .setSmallIcon(android.R.drawable.stat_sys_upload)
                .setContentTitle(title)
                .setContentText(text)
                .setOngoing(state.processRunning)
                .setContentIntent(pending)
                .build();
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return;
        }
        NotificationChannel channel = new NotificationChannel(
                CHANNEL_ID, getString(R.string.notification_channel_name), NotificationManager.IMPORTANCE_LOW);
        channel.setDescription(getString(R.string.notification_channel_description));
        NotificationManager manager = getSystemService(NotificationManager.class);
        if (manager != null) {
            manager.createNotificationChannel(channel);
        }
    }

    private void setError(String error) {
        synchronized (stateLock) {
            lastError = error;
        }
        appendLog(error);
        updateNotification();
    }

    private void appendLog(String line) {
        synchronized (stateLock) {
            logLines.addLast(line);
            while (logLines.size() > MAX_LOG_LINES) {
                logLines.removeFirst();
            }
        }
    }

    private String joinLogs() {
        StringBuilder result = new StringBuilder();
        for (String line : logLines) {
            result.append(line).append('\n');
        }
        return result.toString();
    }

    private static String redact(String line) {
        Matcher matcher = SECRET.matcher(line);
        return matcher.replaceAll("$1$2<redacted>");
    }
}
