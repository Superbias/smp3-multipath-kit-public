package com.superbias.smp3.android;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.os.Binder;
import android.os.IBinder;

import java.util.List;

/** One foreground supervisor for all unmodified, independently running SMP3 children. */
public final class Smp3Service extends Service implements RuntimeManager.Listener {
    public static final String ACTION_PREPARE = "com.superbias.smp3.android.PREPARE";
    public static final String ACTION_STOP = "com.superbias.smp3.android.STOP";

    private static final String CHANNEL_ID = "smp3-runtime";
    private static final int NOTIFICATION_ID = 1901;

    private final IBinder binder = new LocalBinder();
    private RuntimeManager runtimeManager;
    private boolean destroyed;

    public final class LocalBinder extends Binder {
        public Smp3Service service() {
            return Smp3Service.this;
        }
    }

    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannel();
        runtimeManager = new RuntimeManager(this, this);
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && ACTION_STOP.equals(intent.getAction())) {
            if (runtimeManager != null) runtimeManager.shutdown();
            stopForeground(STOP_FOREGROUND_REMOVE);
            stopSelf(startId);
            return START_NOT_STICKY;
        }
        if (runtimeManager != null && runtimeManager.hasActiveInstances()) ensureForeground();
        return START_NOT_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return binder;
    }

    public void reloadInstances() {
        if (runtimeManager != null) runtimeManager.reloadInstances();
    }

    public void startInstance(String instanceId) {
        if (runtimeManager == null) return;
        ensureForeground();
        runtimeManager.startInstance(instanceId);
    }

    public void stopInstance(String instanceId) {
        if (runtimeManager != null) runtimeManager.stopInstance(instanceId);
    }

    public void restartInstance(String instanceId) {
        if (runtimeManager == null) return;
        ensureForeground();
        runtimeManager.restartInstance(instanceId);
    }

    public void startAll() {
        if (runtimeManager == null) return;
        ensureForeground();
        runtimeManager.startAll();
    }

    public void stopAll() {
        if (runtimeManager != null) runtimeManager.stopAll();
    }

    public List<RuntimeManager.RuntimeSnapshot> snapshots() {
        return runtimeManager == null ? java.util.Collections.emptyList() : runtimeManager.snapshots();
    }

    public RuntimeManager.RuntimeSnapshot snapshot(String instanceId) {
        return runtimeManager == null ? null : runtimeManager.snapshot(instanceId);
    }

    public void clearLogs(String instanceId) {
        if (runtimeManager != null) runtimeManager.clearLogs(instanceId);
    }

    public void clearAllLogs() {
        if (runtimeManager != null) runtimeManager.clearAllLogs();
    }

    @Override
    public void onRuntimeChanged() {
        if (destroyed || runtimeManager == null) return;
        if (runtimeManager.hasActiveInstances()) {
            ensureForeground();
            updateNotification();
        } else {
            stopForeground(STOP_FOREGROUND_REMOVE);
        }
    }

    @Override
    public void onDestroy() {
        destroyed = true;
        if (runtimeManager != null) runtimeManager.shutdown();
        stopForeground(STOP_FOREGROUND_REMOVE);
        super.onDestroy();
    }

    private void ensureForeground() {
        startForeground(NOTIFICATION_ID, buildNotification());
    }

    private void updateNotification() {
        NotificationManager manager = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (manager != null) manager.notify(NOTIFICATION_ID, buildNotification());
    }

    private Notification buildNotification() {
        int running = 0;
        StringBuilder names = new StringBuilder();
        if (runtimeManager != null) {
            for (RuntimeManager.RuntimeSnapshot snapshot : runtimeManager.snapshots()) {
                if (!snapshot.processRunning && snapshot.state != RuntimeManager.RuntimeState.STARTING
                        && snapshot.state != RuntimeManager.RuntimeState.STOPPING) continue;
                running++;
                if (names.length() > 0) names.append(", ");
                names.append(snapshot.name);
            }
        }
        String title = running == 0 ? "SMP3 Android" : "SMP3 • " + running + " running";
        String text = running == 0 ? "Ready to start" : names.toString();
        Intent launch = new Intent(this, MainActivity.class);
        int flags = PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE;
        PendingIntent pending = PendingIntent.getActivity(this, 0, launch, flags);
        return new Notification.Builder(this, CHANNEL_ID)
                .setSmallIcon(android.R.drawable.stat_sys_upload)
                .setContentTitle(title)
                .setContentText(text)
                .setOngoing(running > 0)
                .setContentIntent(pending)
                .build();
    }

    private void createNotificationChannel() {
        NotificationChannel channel = new NotificationChannel(CHANNEL_ID,
                getString(R.string.notification_channel_name), NotificationManager.IMPORTANCE_LOW);
        channel.setDescription(getString(R.string.notification_channel_description));
        NotificationManager manager = getSystemService(NotificationManager.class);
        if (manager != null) manager.createNotificationChannel(channel);
    }
}
