package com.superbias.smp3.android;

import android.Manifest;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.graphics.Typeface;
import android.graphics.drawable.GradientDrawable;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.text.InputType;
import android.view.Gravity;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.FrameLayout;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Multi-instance Android supervisor UI; it never implements proxy protocols. */
public final class MainActivity extends Activity {
    private static final int NOTIFICATION_REQUEST = 1902;
    private static final int BACKGROUND = Color.rgb(5, 16, 31);
    private static final int SURFACE = Color.rgb(13, 30, 52);
    private static final int SURFACE_RAISED = Color.rgb(20, 39, 73);
    private static final int BORDER = Color.rgb(36, 72, 111);
    private static final int TEXT = Color.rgb(239, 245, 255);
    private static final int MUTED = Color.rgb(143, 171, 207);
    private static final int BLUE = Color.rgb(78, 190, 255);
    private static final int PURPLE = Color.rgb(112, 92, 255);
    private static final int GREEN = Color.rgb(66, 226, 139);
    private static final int AMBER = Color.rgb(255, 202, 92);
    private static final int RED = Color.rgb(255, 111, 125);
    private static final int BLOCK_GAP_DP = 12;
    private static final int INLINE_GAP_DP = 8;
    private static final int MAX_INSTANCES = 8;

    private final Handler handler = new Handler(Looper.getMainLooper());
    private final Runnable refresh = new Runnable() {
        @Override public void run() {
            refreshUi();
            handler.postDelayed(this, 750);
        }
    };

    private FrameLayout pageHost;
    private LinearLayout bottomNav;
    private TextView summary;
    private TextView[] navLabels;
    private LinearLayout instanceList;
    private TextView logText;
    private EditText logSearch;
    private String logFilter = "All";
    private String logInstanceFilter = "All Instances";
    private int selectedPage;
    private String detailId;
    private String editingId;
    private EditorFields editorFields;
    private List<Smp3Instance> instances = new ArrayList<>();
    private Map<String, RuntimeManager.RuntimeSnapshot> snapshots = new HashMap<>();
    private Smp3Service service;
    private boolean bound;
    private Runnable pendingServiceAction;

    private final ServiceConnection connection = new ServiceConnection() {
        @Override public void onServiceConnected(ComponentName name, IBinder binder) {
            service = ((Smp3Service.LocalBinder) binder).service();
            bound = true;
            service.reloadInstances();
            if (pendingServiceAction != null) {
                Runnable action = pendingServiceAction;
                pendingServiceAction = null;
                action.run();
            }
            refreshUi();
        }

        @Override public void onServiceDisconnected(ComponentName name) {
            service = null;
            bound = false;
            Toast.makeText(MainActivity.this, "SMP3 supervisor disconnected", Toast.LENGTH_SHORT).show();
            refreshUi();
        }
    };

    @Override protected void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().setStatusBarColor(BACKGROUND);
        getWindow().setNavigationBarColor(BACKGROUND);
        setContentView(buildRoot());
        instances = InstanceStore.load(this);
        bindService(new Intent(this, Smp3Service.class), connection, BIND_AUTO_CREATE);
        if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, NOTIFICATION_REQUEST);
        }
        refreshUi();
    }

    @Override protected void onResume() {
        super.onResume();
        handler.post(refresh);
    }

    @Override protected void onPause() {
        handler.removeCallbacks(refresh);
        super.onPause();
    }

    @Override protected void onDestroy() {
        if (bound) {
            unbindService(connection);
            bound = false;
        }
        super.onDestroy();
    }

    @Override public void onBackPressed() {
        if (detailId != null || editingId != null) {
            showPage(0);
            return;
        }
        super.onBackPressed();
    }

    private View buildRoot() {
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setBackgroundColor(BACKGROUND);
        root.addView(buildHeader(), new LinearLayout.LayoutParams(-1, dp(82)));
        pageHost = new FrameLayout(this);
        pageHost.setPadding(dp(14), 0, dp(14), 0);
        root.addView(pageHost, new LinearLayout.LayoutParams(-1, 0, 1f));
        bottomNav = buildBottomNavigation();
        root.addView(bottomNav, new LinearLayout.LayoutParams(-1, dp(72)));
        showPage(0);
        return root;
    }

    private View buildHeader() {
        LinearLayout header = new LinearLayout(this);
        header.setGravity(Gravity.CENTER_VERTICAL);
        header.setPadding(dp(20), dp(14), dp(18), dp(10));
        LinearLayout titles = new LinearLayout(this);
        titles.setOrientation(LinearLayout.VERTICAL);
        TextView brand = heading("SMP3", 25);
        brand.setTextColor(BLUE);
        titles.addView(brand);
        titles.addView(label("Android Standalone", 12, MUTED));
        header.addView(titles, new LinearLayout.LayoutParams(0, -2, 1f));
        summary = label("0 running · 0 instances", 11, MUTED);
        summary.setGravity(Gravity.CENTER_VERTICAL | Gravity.END);
        header.addView(summary, new LinearLayout.LayoutParams(dp(145), -1));
        return header;
    }

    private LinearLayout buildBottomNavigation() {
        LinearLayout nav = new LinearLayout(this);
        nav.setGravity(Gravity.CENTER);
        nav.setPadding(dp(8), dp(5), dp(8), dp(7));
        nav.setBackground(roundBackground(Color.rgb(7, 22, 40), Color.rgb(25, 52, 84), 0));
        String[] names = {"Instances", "Logs", "Settings", "About"};
        navLabels = new TextView[names.length];
        for (int i = 0; i < names.length; i++) {
            final int page = i;
            TextView item = label(names[i], 11, MUTED);
            item.setGravity(Gravity.CENTER);
            item.setPadding(dp(4), 0, dp(4), 0);
            item.setOnClickListener(view -> showPage(page));
            navLabels[i] = item;
            nav.addView(item, new LinearLayout.LayoutParams(0, -1, 1f));
        }
        return nav;
    }

    private void showPage(int page) {
        selectedPage = page;
        detailId = null;
        editingId = null;
        editorFields = null;
        if (pageHost == null) return;
        pageHost.removeAllViews();
        View pageView;
        if (page == 0) pageView = buildInstancesPage();
        else if (page == 1) pageView = buildLogsPage();
        else if (page == 2) pageView = buildSettingsPage();
        else pageView = buildAboutPage();
        pageHost.addView(pageView, new FrameLayout.LayoutParams(-1, -1));
        updateNav(page);
    }

    private void updateNav(int page) {
        if (navLabels == null) return;
        for (int i = 0; i < navLabels.length; i++) navLabels[i].setTextColor(i == page ? BLUE : MUTED);
    }

    private View buildInstancesPage() {
        LinearLayout content = pageContent();
        LinearLayout titleRow = new LinearLayout(this);
        titleRow.setGravity(Gravity.CENTER_VERTICAL);
        titleRow.addView(sectionTitle("Instances", "Independent SMP3 runtimes managed by one supervisor"),
                new LinearLayout.LayoutParams(0, -2, 1f));
        Button add = primaryButton("＋ Add");
        titleRow.addView(add, new LinearLayout.LayoutParams(dp(92), dp(44)));
        content.addView(titleRow);
        add.setOnClickListener(view -> showEditor(null));

        LinearLayout controls = new LinearLayout(this);
        Button startAll = primaryButton("▶  Start enabled");
        Button stopAll = secondaryButton("■  Stop all");
        controls.addView(startAll, buttonParams(1f));
        controls.addView(stopAll, buttonParams(1f));
        content.addView(controls, cardParams());
        startAll.setOnClickListener(view -> requestService(() -> service.startAll()));
        stopAll.setOnClickListener(view -> confirmStopAll());

        instanceList = new LinearLayout(this);
        instanceList.setOrientation(LinearLayout.VERTICAL);
        content.addView(instanceList, cardParams());
        renderInstanceList();
        return scrollPage(content);
    }

    private void renderInstanceList() {
        if (instanceList == null) return;
        instanceList.removeAllViews();
        if (instances.isEmpty()) {
            LinearLayout empty = cardLayout(SURFACE_RAISED, BORDER);
            empty.setGravity(Gravity.CENTER);
            empty.setPadding(dp(22), dp(30), dp(22), dp(30));
            empty.addView(heading("No SMP3 instances yet", 18));
            empty.addView(label("Create your first standalone instance.", 12, MUTED));
            Button add = primaryButton("＋  Add Instance");
            empty.addView(add, new LinearLayout.LayoutParams(-1, dp(48)));
            add.setOnClickListener(view -> showEditor(null));
            instanceList.addView(empty, cardParams());
            return;
        }
        for (Smp3Instance instance : instances) addInstanceCard(instance);
    }

    private void addInstanceCard(Smp3Instance instance) {
        RuntimeManager.RuntimeSnapshot state = snapshotFor(instance);
        LinearLayout card = cardLayout(SURFACE_RAISED, BORDER);
        card.setPadding(dp(16), dp(15), dp(16), dp(15));
        LinearLayout top = new LinearLayout(this);
        top.setGravity(Gravity.CENTER_VERTICAL);
        TextView dot = label("●", 18, stateColor(state));
        top.addView(dot, new LinearLayout.LayoutParams(dp(28), dp(30)));
        LinearLayout names = new LinearLayout(this);
        names.setOrientation(LinearLayout.VERTICAL);
        TextView name = heading(instance.name, 17);
        name.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        names.addView(name);
        names.addView(label(stateLabel(state), 11, stateColor(state)));
        top.addView(names, new LinearLayout.LayoutParams(0, -2, 1f));
        Button edit = compactButton("Details");
        top.addView(edit, new LinearLayout.LayoutParams(dp(84), dp(40)));
        card.addView(top);
        card.addView(label("Local SOCKS", 10, MUTED));
        card.addView(valueText(instance.localAddress()));
        card.addView(label("Server  " + instance.serverEndpoint, 11, MUTED));
        card.addView(label("Paths  " + instance.carriers.size() + " configured", 11, MUTED));
        LinearLayout actions = new LinearLayout(this);
        Button action = state.processRunning || state.state == RuntimeManager.RuntimeState.STARTING
                ? secondaryButton("■  Stop") : primaryButton("▶  Start");
        actions.addView(action, buttonParams(1f));
        if (state.processRunning) {
            Button restart = secondaryButton("↻  Restart");
            actions.addView(restart, buttonParams(1f));
            restart.setOnClickListener(view -> requestService(() -> service.restartInstance(instance.id)));
        }
        card.addView(actions, cardParams());
        action.setOnClickListener(view -> {
            if (state.processRunning || state.state == RuntimeManager.RuntimeState.STARTING) {
                requestService(() -> service.stopInstance(instance.id));
            } else {
                requestService(() -> service.startInstance(instance.id));
            }
        });
        edit.setOnClickListener(view -> showDetail(instance.id));
        instanceList.addView(card, cardParams());
    }

    private View buildLogsPage() {
        LinearLayout content = pageContent();
        content.addView(sectionTitle("Logs", "Per-instance bounded logs from independent native processes"));
        LinearLayout instanceFilters = new LinearLayout(this);
        instanceFilters.setGravity(Gravity.CENTER_VERTICAL);
        addLogInstanceFilter(instanceFilters, "All Instances", true);
        for (Smp3Instance instance : instances) addLogInstanceFilter(instanceFilters, instance.name, false);
        content.addView(instanceFilters, cardParams());
        LinearLayout levelFilters = new LinearLayout(this);
        for (String filter : new String[]{"All", "Info", "Warn", "Error"}) {
            Button button = compactButton(filter);
            levelFilters.addView(button, buttonParams(1f));
            button.setOnClickListener(view -> {
                logFilter = filter;
                renderLogs();
            });
        }
        content.addView(levelFilters, cardParams());
        logSearch = input("Search logs…");
        content.addView(logSearch, cardParams());
        logText = label("No runtime logs yet", 12, Color.rgb(188, 211, 239));
        logText.setTextIsSelectable(true);
        logText.setTypeface(Typeface.MONOSPACE);
        logText.setGravity(Gravity.TOP | Gravity.START);
        logText.setBackground(roundBackground(Color.rgb(8, 21, 38), BORDER, 18));
        logText.setPadding(dp(12), dp(12), dp(12), dp(12));
        content.addView(logText, fixedBlockParams(320));
        LinearLayout controls = new LinearLayout(this);
        Button clear = secondaryButton("⌫  Clear all logs");
        Button copy = secondaryButton("▣  Copy visible");
        controls.addView(clear, buttonParams(1f));
        controls.addView(copy, buttonParams(1f));
        content.addView(controls, cardParams());
        clear.setOnClickListener(view -> requestService(() -> service.clearAllLogs()));
        copy.setOnClickListener(view -> {
            ClipboardManager clipboard = (ClipboardManager) getSystemService(CLIPBOARD_SERVICE);
            if (clipboard != null) {
                clipboard.setPrimaryClip(ClipData.newPlainText("SMP3 logs", logText.getText()));
                Toast.makeText(this, "Visible logs copied", Toast.LENGTH_SHORT).show();
            }
        });
        content.addView(note("Logs are capped at 1,000 entries per instance. Passwords, tokens, keys and full sensitive values are redacted."), cardParams());
        renderLogs();
        return scrollPage(content);
    }

    private void addLogInstanceFilter(LinearLayout parent, String name, boolean selected) {
        Button button = compactButton(name);
        parent.addView(button, buttonParams(1f));
        button.setOnClickListener(view -> {
            logInstanceFilter = name;
            renderLogs();
        });
    }

    private void renderLogs() {
        if (logText == null) return;
        String query = logSearch == null ? "" : logSearch.getText().toString().trim().toLowerCase(Locale.ROOT);
        StringBuilder output = new StringBuilder();
        for (RuntimeManager.RuntimeSnapshot snapshot : snapshots.values()) {
            if (!"All Instances".equals(logInstanceFilter) && !snapshot.name.equals(logInstanceFilter)) continue;
            for (RuntimeManager.LogEntry entry : snapshot.logEntries) {
                String line = entry.display();
                String lower = line.toLowerCase(Locale.ROOT);
                if (!query.isEmpty() && !lower.contains(query)) continue;
                if (!matchesLogFilter(entry.level)) continue;
                output.append('[').append(snapshot.name).append("] ").append(line).append('\n');
            }
        }
        logText.setText(output.length() == 0 ? "No matching logs" : output.toString());
    }

    private boolean matchesLogFilter(String level) {
        if ("All".equals(logFilter)) return true;
        return logFilter.equalsIgnoreCase(level);
    }

    private View buildSettingsPage() {
        LinearLayout content = pageContent();
        content.addView(sectionTitle("Settings", "Preferences for the Android supervisor"));
        LinearLayout runtime = groupCard("⌘", "Runtime policy", "Safe defaults for multi-instance operation");
        runtime.addView(label("Maximum instances", 11, MUTED));
        runtime.addView(valueText(MAX_INSTANCES + " configured instances maximum"));
        runtime.addView(label("Log retention", 11, MUTED));
        runtime.addView(valueText("1,000 entries per instance"));
        content.addView(runtime, cardParams());
        LinearLayout behavior = groupCard("✓", "Lifecycle", "Controls apply to the app supervisor only");
        CheckBox confirm = new CheckBox(this);
        confirm.setText(R.string.confirm_stop_all);
        confirm.setTextColor(TEXT);
        confirm.setChecked(true);
        behavior.addView(confirm, new LinearLayout.LayoutParams(-1, dp(48)));
        content.addView(behavior, cardParams());
        content.addView(note("Carrier processes and proxy nodes remain external. SMP3 only consumes the local SOCKS5 endpoints you configure per instance."), cardParams());
        content.addView(note("If handshakes repeatedly fail, enable Android Automatic date & time. The app does not change SMP3 wire freshness rules."), cardParams());
        return scrollPage(content);
    }

    private View buildAboutPage() {
        LinearLayout content = pageContent();
        content.addView(sectionTitle("About", "Simple. Independent. Multi-instance ready."));
        LinearLayout hero = cardLayout(GradientDrawable.Orientation.TL_BR, SURFACE_RAISED, Color.rgb(36, 44, 104));
        hero.setPadding(dp(18), dp(18), dp(18), dp(18));
        TextView logo = heading("S3", 26);
        logo.setTextColor(Color.WHITE);
        logo.setGravity(Gravity.CENTER);
        logo.setBackground(gradientBackground(Color.rgb(55, 193, 255), PURPLE, 18));
        hero.addView(logo, new LinearLayout.LayoutParams(dp(64), dp(64)));
        hero.addView(heading("SMP3 Android Standalone", 18));
        hero.addView(label("Independent process supervisor", 12, MUTED));
        content.addView(hero, cardParams());
        LinearLayout architecture = groupCard("◈", "Architecture", "The boundaries are intentional");
        addKeyValue(architecture, "Version", appVersion());
        addKeyValue(architecture, "Runtime", "1 Foreground Service + N processes");
        addKeyValue(architecture, "Payload", "libsmp3-client.so · arm64-v8a");
        addKeyValue(architecture, "Proxy boundary", "Standard SOCKS5");
        addKeyValue(architecture, "VPN / TUN", "Not used");
        content.addView(architecture, cardParams());
        content.addView(note("SMP3 does not implement VLESS, Reality, Snell, Hysteria2 or other carrier protocols. Configure those in your external proxy core and expose local SOCKS5 listeners."), cardParams());
        return scrollPage(content);
    }

    private View buildDetailPage(String instanceId) {
        Smp3Instance instance = findInstance(instanceId);
        if (instance == null) {
            showPage(0);
            return pageContent();
        }
        RuntimeManager.RuntimeSnapshot state = snapshotFor(instance);
        LinearLayout content = pageContent();
        LinearLayout backRow = new LinearLayout(this);
        Button back = secondaryButton("‹  Instances");
        backRow.addView(back, new LinearLayout.LayoutParams(dp(120), dp(42)));
        backRow.addView(sectionTitle(instance.name, "Instance details and lifecycle"), new LinearLayout.LayoutParams(0, -2, 1f));
        content.addView(backRow);
        back.setOnClickListener(view -> showPage(0));
        LinearLayout status = cardLayout(SURFACE_RAISED, BORDER);
        status.setPadding(dp(16), dp(16), dp(16), dp(16));
        TextView statusTitle = heading(stateLabel(state), 21);
        statusTitle.setTextColor(stateColor(state));
        status.addView(statusTitle);
        status.addView(label(state.socksReady ? "SOCKS Ready" : "Process and SOCKS status are reported separately", 12, MUTED));
        if (state.error != null && !state.error.isEmpty()) status.addView(label(state.error, 12, RED));
        content.addView(status, cardParams());
        LinearLayout connection = groupCard("↗", "Connection", "Configuration owned by this instance");
        addKeyValue(connection, "Local SOCKS", instance.localAddress());
        addKeyValue(connection, "Server", instance.serverEndpoint);
        for (int i = 0; i < instance.carriers.size(); i++) {
            CarrierEndpoint carrier = instance.carriers.get(i);
            addKeyValue(connection, carrier.name, carrier.address());
        }
        content.addView(connection, cardParams());
        LinearLayout runtime = groupCard("◉", "Runtime", "Independent native child process");
        addKeyValue(runtime, "State", state.state.toString());
        addKeyValue(runtime, "Managed ID", state.processIdentity < 0 ? "—" : String.valueOf(state.processIdentity));
        addKeyValue(runtime, "Start time", state.startTime == 0 ? "—" : String.valueOf(state.startTime));
        content.addView(runtime, cardParams());
        LinearLayout actions = new LinearLayout(this);
        Button start = primaryButton("▶  Start");
        Button stop = secondaryButton("■  Stop");
        Button restart = secondaryButton("↻  Restart");
        actions.addView(start, buttonParams(1f));
        actions.addView(stop, buttonParams(1f));
        actions.addView(restart, buttonParams(1f));
        content.addView(actions, cardParams());
        start.setEnabled(!state.processRunning && state.state != RuntimeManager.RuntimeState.STARTING);
        stop.setEnabled(state.processRunning || state.state == RuntimeManager.RuntimeState.STARTING);
        restart.setEnabled(state.processRunning);
        start.setOnClickListener(view -> requestService(() -> service.startInstance(instance.id)));
        stop.setOnClickListener(view -> requestService(() -> service.stopInstance(instance.id)));
        restart.setOnClickListener(view -> requestService(() -> service.restartInstance(instance.id)));
        LinearLayout secondary = new LinearLayout(this);
        Button edit = secondaryButton("✎  Edit configuration");
        Button logs = secondaryButton("▤  View logs");
        secondary.addView(edit, buttonParams(1f));
        secondary.addView(logs, buttonParams(1f));
        content.addView(secondary, cardParams());
        edit.setOnClickListener(view -> showEditor(instance.id));
        logs.setOnClickListener(view -> showPage(1));
        Button delete = secondaryButton("Delete instance");
        content.addView(delete, cardParams());
        delete.setTextColor(RED);
        delete.setOnClickListener(view -> confirmDelete(instance.id));
        return scrollPage(content);
    }

    private void showDetail(String instanceId) {
        detailId = instanceId;
        editingId = null;
        pageHost.removeAllViews();
        pageHost.addView(buildDetailPage(instanceId), new FrameLayout.LayoutParams(-1, -1));
        updateNav(0);
    }

    private void showEditor(String instanceId) {
        if (instanceId == null && instances.size() >= MAX_INSTANCES) {
            showError("You can manage up to " + MAX_INSTANCES + " instances");
            return;
        }
        editingId = instanceId;
        detailId = null;
        pageHost.removeAllViews();
        pageHost.addView(buildEditorPage(instanceId), new FrameLayout.LayoutParams(-1, -1));
        updateNav(0);
    }

    private View buildEditorPage(String instanceId) {
        final Smp3Instance target;
        if (instanceId == null) {
            int local = PortValidator.recommendLocalPort(instances);
            int carrier = PortValidator.recommendCarrierBasePort(instances);
            target = Smp3Instance.defaultInstance("New Instance", local, carrier);
        } else {
            Smp3Instance found = findInstance(instanceId);
            target = found == null ? new Smp3Instance() : found.copy();
        }
        LinearLayout content = pageContent();
        LinearLayout title = new LinearLayout(this);
        Button back = secondaryButton("‹  Back");
        title.addView(back, new LinearLayout.LayoutParams(dp(90), dp(42)));
        title.addView(sectionTitle(instanceId == null ? "New instance" : "Edit instance", "Each instance has its own process, ports and server"),
                new LinearLayout.LayoutParams(0, -2, 1f));
        content.addView(title);
        back.setOnClickListener(view -> {
            if (instanceId == null) showPage(0);
            else showDetail(instanceId);
        });

        LinearLayout general = groupCard("◇", "General", "Stable ID is retained when the name changes");
        editorFields = new EditorFields();
        editorFields.name = field(general, "Name", target.name);
        editorFields.enabled = new CheckBox(this);
        editorFields.enabled.setText(R.string.start_enabled_instances);
        editorFields.enabled.setTextColor(TEXT);
        editorFields.enabled.setChecked(target.enabled);
        general.addView(editorFields.enabled, new LinearLayout.LayoutParams(-1, dp(46)));
        content.addView(general, cardParams());

        LinearLayout local = groupCard("↗", "Local SOCKS", "Loopback-only application entry point");
        editorFields.localHost = field(local, "Host", target.localSocksHost);
        editorFields.localPort = field(local, "Port", target.localSocksPort);
        content.addView(local, cardParams());

        LinearLayout server = groupCard("▣", "Server", "SMP3 protocol endpoint");
        editorFields.server = field(server, "Endpoint", target.serverEndpoint);
        editorFields.password = field(server, "Password", target.smp3Password);
        editorFields.password.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_PASSWORD);
        Button showPassword = secondaryButton("Show / hide password");
        server.addView(showPassword, new LinearLayout.LayoutParams(-1, dp(42)));
        showPassword.setOnClickListener(view -> {
            int selection = editorFields.password.getSelectionStart();
            boolean hidden = editorFields.password.getInputType()
                    == (InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_PASSWORD);
            editorFields.password.setInputType(InputType.TYPE_CLASS_TEXT | (hidden ? InputType.TYPE_TEXT_VARIATION_VISIBLE_PASSWORD : InputType.TYPE_TEXT_VARIATION_PASSWORD));
            editorFields.password.setSelection(Math.max(0, selection));
        });
        content.addView(server, cardParams());

        LinearLayout carriers = groupCard("⌘", "Carriers", "External proxy core SOCKS5 listeners; current runtime supports two");
        CarrierEndpoint carrierA = target.carriers.size() > 0 ? target.carriers.get(0) : new CarrierEndpoint();
        CarrierEndpoint carrierB = target.carriers.size() > 1 ? target.carriers.get(1) : new CarrierEndpoint();
        editorFields.carrierAName = field(carriers, "Carrier 1 name", carrierA.name);
        editorFields.carrierAHost = field(carriers, "Carrier 1 host", carrierA.host);
        editorFields.carrierAPort = field(carriers, "Carrier 1 port", carrierA.port);
        editorFields.carrierBName = field(carriers, "Carrier 2 name", carrierB.name);
        editorFields.carrierBHost = field(carriers, "Carrier 2 host", carrierB.host);
        editorFields.carrierBPort = field(carriers, "Carrier 2 port", carrierB.port);
        content.addView(carriers, cardParams());

        LinearLayout buttons = new LinearLayout(this);
        Button save = primaryButton("▣  Save");
        Button saveStart = secondaryButton("▶  Save & start");
        buttons.addView(save, buttonParams(1f));
        buttons.addView(saveStart, buttonParams(1f));
        content.addView(buttons, cardParams());
        save.setOnClickListener(view -> saveEditor(target, false));
        saveStart.setOnClickListener(view -> saveEditor(target, true));
        content.addView(note("Passwords are stored only in the app-private sandbox. The Android app does not manage node subscriptions or proxy-core configuration."), cardParams());
        return scrollPage(content);
    }

    private void saveEditor(Smp3Instance original, boolean startAfterSave) {
        Smp3Instance updated = original.copy();
        updated.name = editorFields.name.getText().toString().trim();
        updated.enabled = editorFields.enabled.isChecked();
        updated.localSocksHost = editorFields.localHost.getText().toString().trim();
        updated.localSocksPort = editorFields.localPort.getText().toString().trim();
        updated.serverEndpoint = editorFields.server.getText().toString().trim();
        updated.smp3Password = editorFields.password.getText().toString();
        updated.carriers.clear();
        updated.carriers.add(new CarrierEndpoint(original.carriers.size() > 0 ? original.carriers.get(0).id : null,
                editorFields.carrierAName.getText().toString().trim(), editorFields.carrierAHost.getText().toString().trim(),
                editorFields.carrierAPort.getText().toString().trim()));
        updated.carriers.add(new CarrierEndpoint(original.carriers.size() > 1 ? original.carriers.get(1).id : null,
                editorFields.carrierBName.getText().toString().trim(), editorFields.carrierBHost.getText().toString().trim(),
                editorFields.carrierBPort.getText().toString().trim()));
        String error = updated.validationError();
        if (error == null) error = PortValidator.validate(instances, updated);
        if (error != null) {
            showError(error);
            return;
        }
        RuntimeManager.RuntimeSnapshot running = snapshots.get(updated.id);
        if (running != null && running.processRunning) {
            showError("Stop this instance before editing its configuration");
            return;
        }
        List<Smp3Instance> next = new ArrayList<>();
        boolean replaced = false;
        for (Smp3Instance instance : instances) {
            if (instance.id.equals(updated.id)) {
                next.add(updated);
                replaced = true;
            } else next.add(instance);
        }
        if (!replaced) next.add(updated);
        try {
            InstanceStore.save(this, next);
            instances = next;
            if (service != null) service.reloadInstances();
            Toast.makeText(this, "Instance saved", Toast.LENGTH_SHORT).show();
            if (startAfterSave) {
                final String id = updated.id;
                requestService(() -> service.startInstance(id));
            }
            showPage(0);
        } catch (IOException | org.json.JSONException exception) {
            showError("Unable to save instance configuration");
        }
    }

    private void confirmStopAll() {
        new AlertDialog.Builder(this).setTitle("Stop all instances?")
                .setMessage("This gracefully stops every managed SMP3 process.")
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Stop all", (dialog, which) -> requestService(() -> service.stopAll()))
                .show();
    }

    private void confirmDelete(String id) {
        Smp3Instance instance = findInstance(id);
        if (instance == null) return;
        new AlertDialog.Builder(this).setTitle("Delete \"" + instance.name + "\"?")
                .setMessage("This removes the SMP3 instance configuration. External proxy-core settings are not changed.")
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Delete", (dialog, which) -> deleteWhenStopped(id))
                .show();
    }

    private void deleteWhenStopped(final String id) {
        RuntimeManager.RuntimeSnapshot state = snapshots.get(id);
        if (state != null && isActive(state)) {
            if (!state.processRunning) {
                showError("Wait for this instance to finish starting or stopping");
                return;
            }
            requestService(() -> service.stopInstance(id));
            handler.postDelayed(new Runnable() {
                @Override public void run() {
                    refreshUi();
                    RuntimeManager.RuntimeSnapshot current = snapshots.get(id);
                    if (current != null && isActive(current)) handler.postDelayed(this, 250);
                    else deleteNow(id);
                }
            }, 250);
            return;
        }
        deleteNow(id);
    }

    private void deleteNow(String id) {
        List<Smp3Instance> next = new ArrayList<>();
        for (Smp3Instance instance : instances) if (!instance.id.equals(id)) next.add(instance);
        try {
            InstanceStore.save(this, next);
            InstanceStore.deleteConfig(this, id);
            instances = next;
            if (service != null) service.reloadInstances();
            Toast.makeText(this, "Instance deleted", Toast.LENGTH_SHORT).show();
            showPage(0);
        } catch (IOException | org.json.JSONException exception) {
            showError("Unable to delete instance");
        }
    }

    private void requestService(Runnable action) {
        if (service != null) {
            action.run();
            return;
        }
        pendingServiceAction = action;
        Intent intent = new Intent(this, Smp3Service.class);
        startForegroundService(intent);
        if (!bound) bindService(intent, connection, BIND_AUTO_CREATE);
    }

    private void refreshUi() {
        if (isFinishing()) return;
        instances = InstanceStore.load(this);
        snapshots = new HashMap<>();
        if (service != null) for (RuntimeManager.RuntimeSnapshot snapshot : service.snapshots()) snapshots.put(snapshot.instanceId, snapshot);
        int running = 0;
        for (RuntimeManager.RuntimeSnapshot snapshot : snapshots.values()) if (snapshot.processRunning) running++;
        if (summary != null) summary.setText(getResources().getQuantityString(
                R.plurals.instances_summary, running, running, instances.size()));
        if (editingId != null) return;
        if (detailId != null) {
            pageHost.removeAllViews();
            pageHost.addView(buildDetailPage(detailId), new FrameLayout.LayoutParams(-1, -1));
        } else if (selectedPage == 0) {
            if (instanceList != null) renderInstanceList();
        } else if (selectedPage == 1) renderLogs();
    }

    private Smp3Instance findInstance(String id) {
        for (Smp3Instance instance : instances) if (instance.id.equals(id)) return instance;
        return null;
    }

    private RuntimeManager.RuntimeSnapshot snapshotFor(Smp3Instance instance) {
        RuntimeManager.RuntimeSnapshot snapshot = snapshots.get(instance.id);
        return snapshot == null ? RuntimeManager.RuntimeSnapshot.idle(instance) : snapshot;
    }

    private String stateLabel(RuntimeManager.RuntimeSnapshot snapshot) {
        if (snapshot.state == RuntimeManager.RuntimeState.SOCKS_READY) return "SOCKS Ready";
        if (snapshot.state == RuntimeManager.RuntimeState.RUNNING) return "Running";
        if (snapshot.state == RuntimeManager.RuntimeState.STARTING) return "Starting";
        if (snapshot.state == RuntimeManager.RuntimeState.STOPPING) return "Stopping";
        if (snapshot.state == RuntimeManager.RuntimeState.ERROR) return "Error";
        return "Stopped";
    }

    private int stateColor(RuntimeManager.RuntimeSnapshot snapshot) {
        if (snapshot.state == RuntimeManager.RuntimeState.ERROR) return RED;
        if (snapshot.processRunning || snapshot.state == RuntimeManager.RuntimeState.SOCKS_READY) return GREEN;
        if (snapshot.state == RuntimeManager.RuntimeState.STARTING || snapshot.state == RuntimeManager.RuntimeState.STOPPING) return AMBER;
        return MUTED;
    }

    private boolean isActive(RuntimeManager.RuntimeSnapshot snapshot) {
        return snapshot.processRunning || snapshot.state == RuntimeManager.RuntimeState.STARTING
                || snapshot.state == RuntimeManager.RuntimeState.STOPPING;
    }

    private void showError(String message) {
        Toast.makeText(this, message, Toast.LENGTH_LONG).show();
    }

    private LinearLayout pageContent() {
        LinearLayout content = new LinearLayout(this);
        content.setOrientation(LinearLayout.VERTICAL);
        content.setPadding(dp(2), dp(2), dp(2), dp(22));
        return content;
    }

    private ScrollView scrollPage(LinearLayout content) {
        ScrollView scroll = new ScrollView(this);
        scroll.setFillViewport(true);
        scroll.setClipToPadding(false);
        scroll.addView(content);
        return scroll;
    }

    private LinearLayout sectionTitle(String title, String subtitle) {
        LinearLayout wrapper = new LinearLayout(this);
        wrapper.setOrientation(LinearLayout.VERTICAL);
        wrapper.setPadding(dp(2), dp(8), dp(2), dp(8));
        wrapper.addView(heading(title, 20));
        wrapper.addView(label(subtitle, 11, MUTED));
        return wrapper;
    }

    private LinearLayout groupCard(String icon, String title, String subtitle) {
        LinearLayout card = cardLayout(SURFACE, BORDER);
        card.setPadding(dp(14), dp(13), dp(14), dp(13));
        LinearLayout header = new LinearLayout(this);
        header.setGravity(Gravity.CENTER_VERTICAL);
        header.addView(label(icon, 21, BLUE), new LinearLayout.LayoutParams(dp(38), dp(34)));
        LinearLayout titles = new LinearLayout(this);
        titles.setOrientation(LinearLayout.VERTICAL);
        TextView titleView = label(title, 14, TEXT);
        titleView.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        titles.addView(titleView);
        titles.addView(label(subtitle, 10, MUTED));
        header.addView(titles, new LinearLayout.LayoutParams(0, -2, 1f));
        card.addView(header);
        return card;
    }

    private LinearLayout infoCard(String icon, String title, String subtitle) {
        LinearLayout card = groupCard(icon, title, subtitle);
        return card;
    }

    private LinearLayout note(String text) {
        LinearLayout card = cardLayout(SURFACE, BORDER);
        card.setPadding(dp(14), dp(12), dp(14), dp(12));
        card.addView(label(text, 12, MUTED));
        return card;
    }

    private void addKeyValue(LinearLayout parent, String key, String value) {
        LinearLayout row = new LinearLayout(this);
        row.setGravity(Gravity.CENTER_VERTICAL);
        row.addView(label(key, 11, MUTED), new LinearLayout.LayoutParams(dp(112), dp(32)));
        row.addView(label(value, 12, TEXT), new LinearLayout.LayoutParams(0, dp(32), 1f));
        parent.addView(row);
    }

    private TextView valueText(String value) {
        TextView view = label(value, 14, TEXT);
        view.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        return view;
    }

    private EditText field(LinearLayout parent, String hint, String value) {
        TextView caption = label(hint, 11, MUTED);
        caption.setPadding(dp(4), dp(9), 0, dp(4));
        parent.addView(caption);
        EditText input = input(value);
        parent.addView(input, new LinearLayout.LayoutParams(-1, dp(44)));
        return input;
    }

    private EditText input(String value) {
        EditText input = new EditText(this);
        input.setSingleLine(true);
        input.setText(value);
        input.setTextSize(14);
        input.setTextColor(TEXT);
        input.setHintTextColor(Color.rgb(100, 131, 171));
        input.setPadding(dp(12), 0, dp(12), 0);
        input.setBackground(roundBackground(Color.rgb(11, 27, 48), BORDER, 12));
        return input;
    }

    private Button primaryButton(String text) {
        Button button = styledButton(text);
        button.setTextColor(Color.WHITE);
        button.setBackground(gradientBackground(BLUE, PURPLE, 16));
        return button;
    }

    private Button secondaryButton(String text) {
        Button button = styledButton(text);
        button.setTextColor(TEXT);
        button.setBackground(roundBackground(Color.rgb(22, 43, 70), BORDER, 16));
        return button;
    }

    private Button compactButton(String text) {
        Button button = styledButton(text);
        button.setTextSize(11);
        button.setTextColor(MUTED);
        button.setBackground(roundBackground(SURFACE, BORDER, 40));
        return button;
    }

    private Button styledButton(String text) {
        Button button = new Button(this);
        button.setText(text);
        button.setTextSize(13);
        button.setAllCaps(false);
        button.setMinHeight(dp(44));
        button.setMinWidth(0);
        button.setPadding(dp(6), 0, dp(6), 0);
        return button;
    }

    private TextView heading(String text, float size) {
        TextView view = label(text, size, TEXT);
        view.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        return view;
    }

    private TextView label(String text, float size, int color) {
        TextView view = new TextView(this);
        view.setText(text);
        view.setTextSize(size);
        view.setTextColor(color);
        return view;
    }

    private LinearLayout cardLayout(int color, int stroke) {
        LinearLayout card = new LinearLayout(this);
        card.setOrientation(LinearLayout.VERTICAL);
        card.setBackground(roundBackground(color, stroke, 18));
        return card;
    }

    private LinearLayout cardLayout(GradientDrawable.Orientation orientation, int start, int end) {
        LinearLayout card = new LinearLayout(this);
        card.setOrientation(LinearLayout.VERTICAL);
        card.setBackground(gradientBackground(orientation, start, end, 18));
        return card;
    }

    private LinearLayout.LayoutParams cardParams() {
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(-1, -2);
        params.setMargins(0, dp(BLOCK_GAP_DP), 0, 0);
        return params;
    }

    private LinearLayout.LayoutParams buttonParams(float weight) {
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(0, dp(48), weight);
        int halfGap = INLINE_GAP_DP / 2;
        params.setMargins(dp(halfGap), 0, dp(halfGap), 0);
        return params;
    }

    private LinearLayout.LayoutParams fixedBlockParams(int height) {
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(-1, dp(height));
        params.setMargins(0, dp(BLOCK_GAP_DP), 0, 0);
        return params;
    }

    private GradientDrawable roundBackground(int color, int stroke, int radius) {
        GradientDrawable drawable = new GradientDrawable();
        drawable.setColor(color);
        drawable.setCornerRadius(dp(radius));
        if (stroke != 0) drawable.setStroke(dp(1), stroke);
        return drawable;
    }

    private GradientDrawable gradientBackground(int start, int end, int radius) {
        return gradientBackground(GradientDrawable.Orientation.LEFT_RIGHT, start, end, radius);
    }

    private GradientDrawable gradientBackground(GradientDrawable.Orientation orientation, int start, int end, int radius) {
        GradientDrawable drawable = new GradientDrawable(orientation, new int[]{start, end});
        drawable.setCornerRadius(dp(radius));
        return drawable;
    }

    private String appVersion() {
        try {
            return getPackageManager().getPackageInfo(getPackageName(), 0).versionName;
        } catch (PackageManager.NameNotFoundException ignored) {
            return "unknown";
        }
    }

    private int dp(int value) {
        return (int) (value * getResources().getDisplayMetrics().density + 0.5f);
    }

    private static final class EditorFields {
        EditText name, localHost, localPort, server, password;
        EditText carrierAName, carrierAHost, carrierAPort;
        EditText carrierBName, carrierBHost, carrierBPort;
        CheckBox enabled;
    }
}
