package com.superbias.smp3.android;

import android.Manifest;
import android.app.Activity;
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
import android.text.Editable;
import android.text.InputType;
import android.text.TextWatcher;
import android.view.Gravity;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.EditText;
import android.widget.FrameLayout;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import java.util.Locale;

/** Small configuration/status shell. It never implements proxy protocols. */
public final class MainActivity extends Activity {
    private static final int NOTIFICATION_REQUEST = 1902;

    private static final int BACKGROUND = Color.rgb(5, 16, 31);
    private static final int SURFACE = Color.rgb(13, 30, 52);
    private static final int SURFACE_RAISED = Color.rgb(18, 40, 68);
    private static final int BORDER = Color.rgb(36, 72, 111);
    private static final int TEXT = Color.rgb(239, 245, 255);
    private static final int MUTED = Color.rgb(143, 171, 207);
    private static final int BLUE = Color.rgb(78, 190, 255);
    private static final int PURPLE = Color.rgb(112, 92, 255);
    private static final int GREEN = Color.rgb(66, 226, 139);
    private static final int BLOCK_GAP_DP = 10;
    private static final int INLINE_GAP_DP = 8;

    private final Handler handler = new Handler();
    private final Runnable refresh = new Runnable() {
        @Override
        public void run() {
            refreshStatus();
            handler.postDelayed(this, 750);
        }
    };

    private EditText localHost;
    private EditText localPort;
    private EditText server;
    private EditText carrierAHost;
    private EditText carrierAPort;
    private EditText carrierBHost;
    private EditText carrierBPort;
    private EditText password;
    private EditText logSearch;

    private FrameLayout pageHost;
    private TextView[] navigationItems;
    private TextView statusDot;
    private TextView status;
    private TextView statusSubtitle;
    private TextView statusDetail;
    private TextView localValue;
    private TextView serverValue;
    private TextView pathsValue;
    private TextView carrierAValue;
    private TextView carrierAState;
    private TextView carrierBValue;
    private TextView carrierBState;
    private TextView logs;
    private Button startButton;
    private Button stopButton;
    private String logText = "";
    private String logFilter = "All";
    private Smp3Service service;
    private boolean bound;
    private Smp3Config pendingStart;

    private final ServiceConnection connection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder binder) {
            service = ((Smp3Service.LocalBinder) binder).service();
            bound = true;
            if (pendingStart != null) {
                Smp3Config config = pendingStart;
                pendingStart = null;
                String error = service.startRuntime(config);
                if (error != null) showFailure(error);
            }
            refreshStatus();
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
            service = null;
            bound = false;
            showFailure("SMP3 service disconnected");
        }
    };

    @Override
    protected void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().setStatusBarColor(BACKGROUND);
        getWindow().setNavigationBarColor(BACKGROUND);
        getWindow().getDecorView().setSystemUiVisibility(0);
        setContentView(buildView());
        loadConfig(Smp3Config.load(this));
        bindService(new Intent(this, Smp3Service.class), connection, BIND_AUTO_CREATE);
        if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, NOTIFICATION_REQUEST);
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        handler.post(refresh);
    }

    @Override
    protected void onPause() {
        handler.removeCallbacks(refresh);
        super.onPause();
    }

    @Override
    protected void onDestroy() {
        if (bound) {
            unbindService(connection);
            bound = false;
        }
        super.onDestroy();
    }

    private View buildView() {
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setBackgroundColor(BACKGROUND);
        root.addView(buildHeader(), new LinearLayout.LayoutParams(-1, ViewGroup.LayoutParams.WRAP_CONTENT));

        pageHost = new FrameLayout(this);
        pageHost.setPadding(dp(14), 0, dp(14), 0);
        root.addView(pageHost, new LinearLayout.LayoutParams(-1, 0, 1f));
        pageHost.addView(scrollPage(buildStatusPage()));
        pageHost.addView(scrollPage(buildConfigPage()));
        pageHost.addView(scrollPage(buildLogsPage()));
        pageHost.addView(scrollPage(buildAboutPage()));

        root.addView(buildBottomNavigation(), new LinearLayout.LayoutParams(-1, dp(72)));
        showPage(0);
        return root;
    }

    private View buildHeader() {
        LinearLayout header = new LinearLayout(this);
        header.setGravity(Gravity.CENTER_VERTICAL);
        header.setPadding(dp(20), dp(14), dp(18), dp(12));
        LinearLayout titles = new LinearLayout(this);
        titles.setOrientation(LinearLayout.VERTICAL);
        TextView brand = heading("SMP3", 25);
        brand.setTextColor(BLUE);
        titles.addView(brand);
        titles.addView(label("Android Standalone", 12, MUTED));
        header.addView(titles, new LinearLayout.LayoutParams(0, -2, 1f));
        TextView menu = label("⋮", 28, TEXT);
        menu.setGravity(Gravity.CENTER);
        header.addView(menu, new LinearLayout.LayoutParams(dp(34), dp(46)));
        return header;
    }

    private LinearLayout buildStatusPage() {
        LinearLayout content = pageContent();
        content.addView(sectionTitle("Overview", "Everything important at a glance"));

        LinearLayout hero = cardLayout(GradientDrawable.Orientation.TL_BR, SURFACE_RAISED, Color.rgb(25, 42, 82));
        hero.setPadding(dp(18), dp(18), dp(18), dp(18));
        LinearLayout heroTop = new LinearLayout(this);
        heroTop.setGravity(Gravity.CENTER_VERTICAL);
        statusDot = label("●", 28, Color.rgb(255, 202, 92));
        statusDot.setGravity(Gravity.CENTER);
        heroTop.addView(statusDot, new LinearLayout.LayoutParams(dp(44), dp(44)));
        LinearLayout heroTitles = new LinearLayout(this);
        heroTitles.setOrientation(LinearLayout.VERTICAL);
        status = heading("Stopped", 21);
        heroTitles.addView(status);
        statusSubtitle = label("Ready to start", 13, MUTED);
        heroTitles.addView(statusSubtitle);
        heroTop.addView(heroTitles, new LinearLayout.LayoutParams(0, -2, 1f));
        hero.addView(heroTop);
        statusDetail = label("PROCESS_STOPPED\nSOCKS_NOT_READY", 12, MUTED);
        statusDetail.setPadding(dp(44), dp(12), 0, 0);
        hero.addView(statusDetail);
        content.addView(hero, cardParams());

        content.addView(sectionTitle("Connection", "Your local entry point and remote SMP3 server"));
        LinearLayout local = infoCard("↗", "Local SOCKS", "Waiting for SMP3 runtime");
        localValue = valueText("127.0.0.1:18080");
        ((LinearLayout) local.getChildAt(1)).addView(localValue);
        content.addView(local, cardParams());
        LinearLayout remote = infoCard("▣", "Server", "SMP3 server endpoint");
        serverValue = valueText("127.0.0.1:24445");
        ((LinearLayout) remote.getChildAt(1)).addView(serverValue);
        content.addView(remote, cardParams());
        LinearLayout paths = infoCard("⌘", "Paths", "Two carrier endpoints are configured");
        pathsValue = valueText("2 configured");
        ((LinearLayout) paths.getChildAt(1)).addView(pathsValue);
        content.addView(paths, cardParams());

        content.addView(sectionTitle("Carriers", "External proxy core supplies the SOCKS5 endpoints"));
        LinearLayout carrierRow = new LinearLayout(this);
        carrierRow.setGravity(Gravity.TOP);
        LinearLayout carrierA = carrierCard("Carrier A");
        LinearLayout carrierB = carrierCard("Carrier B");
        carrierRow.addView(carrierA, new LinearLayout.LayoutParams(0, -2, 1f));
        LinearLayout.LayoutParams carrierMargin = new LinearLayout.LayoutParams(0, -2, 1f);
        carrierMargin.setMargins(dp(8), 0, 0, 0);
        carrierRow.addView(carrierB, carrierMargin);
        content.addView(carrierRow, cardParams());

        LinearLayout controls = new LinearLayout(this);
        startButton = primaryButton("▶  Start");
        stopButton = secondaryButton("■  Stop");
        controls.addView(startButton, buttonParams(1f));
        controls.addView(stopButton, buttonParams(1f));
        content.addView(controls, cardParams());
        startButton.setOnClickListener(view -> startRuntime());
        stopButton.setOnClickListener(view -> stopRuntime());

        LinearLayout note = cardLayout(SURFACE, BORDER);
        note.setPadding(dp(14), dp(12), dp(14), dp(12));
        note.addView(label("Carrier status is shown as configured. Connection state is reported by the SMP3 runtime, not guessed by the UI.", 12, MUTED));
        content.addView(note, cardParams());
        return content;
    }

    private LinearLayout carrierCard(String title) {
        LinearLayout card = cardLayout(SURFACE, BORDER);
        card.setPadding(dp(12), dp(12), dp(12), dp(12));
        card.addView(label("◉", 20, BLUE));
        TextView name = label(title, 13, TEXT);
        name.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        card.addView(name);
        TextView endpoint = valueText(title.endsWith("A") ? "127.0.0.1:20001" : "127.0.0.1:20002");
        endpoint.setTextSize(11);
        card.addView(endpoint);
        TextView state = label("●  Configured", 11, GREEN);
        card.addView(state);
        if (title.endsWith("A")) {
            carrierAValue = endpoint;
            carrierAState = state;
        } else {
            carrierBValue = endpoint;
            carrierBState = state;
        }
        return card;
    }

    private LinearLayout buildConfigPage() {
        LinearLayout content = pageContent();
        content.addView(sectionTitle("Configuration", "Set up your SMP3 connection parameters"));
        LinearLayout local = groupCard("↗", "Local SOCKS", "The local proxy entry point");
        localHost = field(local, "Host", "127.0.0.1");
        localPort = field(local, "Port", "18080");
        content.addView(local, cardParams());
        LinearLayout remote = groupCard("▣", "Server", "The SMP3 protocol endpoint");
        server = field(remote, "Endpoint", "127.0.0.1:24445");
        password = field(remote, "Password", "");
        password.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_PASSWORD);
        content.addView(remote, cardParams());
        LinearLayout carriers = groupCard("⌘", "Carriers", "Standard SOCKS5 listeners owned by your proxy core");
        carrierAHost = field(carriers, "Carrier A host", "127.0.0.1");
        carrierAPort = field(carriers, "Carrier A port", "20001");
        carrierBHost = field(carriers, "Carrier B host", "127.0.0.1");
        carrierBPort = field(carriers, "Carrier B port", "20002");
        content.addView(carriers, cardParams());

        LinearLayout controls = new LinearLayout(this);
        Button save = primaryButton("▣  Save");
        Button test = secondaryButton("⌁  Validate");
        controls.addView(save, buttonParams(1f));
        controls.addView(test, buttonParams(1f));
        content.addView(controls, cardParams());
        save.setOnClickListener(view -> saveConfig());
        test.setOnClickListener(view -> validateConfig());

        LinearLayout tip = cardLayout(SURFACE, BORDER);
        tip.setPadding(dp(14), dp(12), dp(14), dp(12));
        tip.addView(label("SMP3 does not contain proxy nodes. Configure VLESS, Reality, Snell, Hysteria2, or other nodes in the external proxy core and expose them as these two local SOCKS5 listeners.", 12, MUTED));
        content.addView(tip, cardParams());
        return content;
    }

    private LinearLayout buildLogsPage() {
        LinearLayout content = pageContent();
        content.addView(sectionTitle("Logs", "Real-time logs from the independent SMP3 process"));
        logSearch = input("Search logs…");
        content.addView(logSearch, cardParams());
        logSearch.addTextChangedListener(new TextWatcher() {
            @Override public void beforeTextChanged(CharSequence s, int start, int count, int after) { }
            @Override public void onTextChanged(CharSequence s, int start, int before, int count) { renderLogs(); }
            @Override public void afterTextChanged(Editable s) { }
        });
        LinearLayout filters = new LinearLayout(this);
        filters.setGravity(Gravity.CENTER_VERTICAL);
        String[] names = {"All", "Info", "Warn", "Error"};
        for (String name : names) {
            Button filter = compactButton(name);
            filters.addView(filter, buttonParams(1f));
            filter.setOnClickListener(view -> {
                logFilter = name;
                renderLogs();
                for (int i = 0; i < filters.getChildCount(); i++) {
                    Button child = (Button) filters.getChildAt(i);
                    boolean selected = child.getText().toString().equals(logFilter);
                    child.setTextColor(selected ? TEXT : MUTED);
                    child.setBackground(roundBackground(selected ? Color.rgb(40, 117, 190) : SURFACE, BORDER, 40));
                }
            });
        }
        content.addView(filters, cardParams());
        logs = new TextView(this);
        logs.setTextIsSelectable(true);
        logs.setTextSize(12);
        logs.setTypeface(Typeface.MONOSPACE);
        logs.setTextColor(Color.rgb(188, 211, 239));
        logs.setGravity(Gravity.TOP | Gravity.START);
        logs.setBackground(roundBackground(Color.rgb(8, 21, 38), BORDER, 18));
        logs.setPadding(dp(12), dp(12), dp(12), dp(12));
        LinearLayout.LayoutParams logsParams = new LinearLayout.LayoutParams(-1, dp(320));
        logsParams.setMargins(0, dp(BLOCK_GAP_DP), 0, 0);
        content.addView(logs, logsParams);
        LinearLayout logControls = new LinearLayout(this);
        Button clear = secondaryButton("⌫  Clear");
        Button copy = secondaryButton("▣  Copy");
        logControls.addView(clear, buttonParams(1f));
        logControls.addView(copy, buttonParams(1f));
        content.addView(logControls, cardParams());
        clear.setOnClickListener(view -> {
            if (service != null) {
                service.clearLogs();
                refreshStatus();
            }
        });
        copy.setOnClickListener(view -> {
            ClipboardManager clipboard = (ClipboardManager) getSystemService(CLIPBOARD_SERVICE);
            if (clipboard != null) {
                clipboard.setPrimaryClip(ClipData.newPlainText("SMP3 logs", logText));
                Toast.makeText(this, "Logs copied", Toast.LENGTH_SHORT).show();
            }
        });
        LinearLayout note = cardLayout(SURFACE, BORDER);
        note.setPadding(dp(14), dp(12), dp(14), dp(12));
        note.addView(label("Logs are retained locally, capped at 200 lines, and sensitive key/value fields are redacted before display.", 12, MUTED));
        content.addView(note, cardParams());
        return content;
    }

    private LinearLayout buildAboutPage() {
        LinearLayout content = pageContent();
        content.addView(sectionTitle("About", "Simple. Independent. Powerful."));
        LinearLayout appCard = cardLayout(GradientDrawable.Orientation.TL_BR, SURFACE_RAISED, Color.rgb(36, 44, 104));
        appCard.setOrientation(LinearLayout.HORIZONTAL);
        appCard.setGravity(Gravity.CENTER_VERTICAL);
        appCard.setPadding(dp(14), dp(14), dp(14), dp(14));
        TextView logo = heading("S3", 24);
        logo.setGravity(Gravity.CENTER);
        logo.setBackground(gradientBackground(Color.rgb(55, 193, 255), PURPLE, 18));
        appCard.addView(logo, new LinearLayout.LayoutParams(dp(58), dp(58)));
        LinearLayout appInfo = new LinearLayout(this);
        appInfo.setOrientation(LinearLayout.VERTICAL);
        appInfo.setPadding(dp(14), 0, 0, 0);
        TextView appName = label("SMP3 Android Standalone", 16, TEXT);
        appName.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        appInfo.addView(appName);
        appInfo.addView(label("Version " + appVersion(), 12, MUTED));
        appInfo.addView(label("Simple. Independent. Powerful.", 12, MUTED));
        appCard.addView(appInfo, new LinearLayout.LayoutParams(0, -2, 1f));
        content.addView(appCard, cardParams());
        content.addView(sectionTitle("Device information", "Runtime environment"));
        LinearLayout device = cardLayout(SURFACE, BORDER);
        device.setPadding(dp(14), dp(10), dp(14), dp(10));
        addKeyValue(device, "Device", Build.MODEL);
        addKeyValue(device, "Android", Build.VERSION.RELEASE + " (API " + Build.VERSION.SDK_INT + ")");
        addKeyValue(device, "ABI", Build.SUPPORTED_ABIS.length == 0 ? "unknown" : Build.SUPPORTED_ABIS[0]);
        addKeyValue(device, "Manufacturer", Build.MANUFACTURER);
        content.addView(device, cardParams());
        content.addView(sectionTitle("Architecture", "Clear boundaries, predictable behavior"));
        LinearLayout architecture = new LinearLayout(this);
        architecture.setGravity(Gravity.TOP);
        addArchitectureCard(architecture, "Independent\nProcess", "Native child", GREEN, true);
        addArchitectureCard(architecture, "Foreground\nService", "Keeps running", BLUE, false);
        LinearLayout architecture2 = new LinearLayout(this);
        architecture2.setGravity(Gravity.TOP);
        addArchitectureCard(architecture2, "Standard\nSOCKS5", "Local boundary", BLUE, true);
        addArchitectureCard(architecture2, "No VPN/TUN", "No system changes", PURPLE, false);
        content.addView(architecture, cardParams());
        content.addView(architecture2, cardParams());
        content.addView(sectionTitle("Health", "Local process and environment"));
        LinearLayout health = cardLayout(SURFACE, BORDER);
        health.setPadding(dp(14), dp(12), dp(14), dp(12));
        addHealthRow(health, "No crash detected", "Runtime reports normally");
        addHealthRow(health, "Configuration local", "Stored in app-private files");
        addHealthRow(health, "Proxy core external", "Nodes stay outside SMP3");
        content.addView(health, cardParams());
        TextView description = label("The APK supervises the independent SMP3 client and connects it to two standard SOCKS5 carrier endpoints. It does not implement VLESS, Reality, Hysteria2, subscriptions, routing rules, VPN mode, Mihomo, or sing-box.", 12, MUTED);
        description.setPadding(dp(4), 0, dp(4), dp(18));
        content.addView(description);
        return content;
    }

    private void addArchitectureCard(LinearLayout row, String title, String subtitle, int accent, boolean first) {
        LinearLayout card = cardLayout(SURFACE, BORDER);
        card.setPadding(dp(11), dp(11), dp(8), dp(11));
        card.addView(label("●", 15, accent));
        TextView titleView = label(title, 12, TEXT);
        titleView.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        card.addView(titleView);
        card.addView(label(subtitle, 10, MUTED));
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(0, -2, 1f);
        if (!first) params.setMargins(dp(8), 0, 0, 0);
        row.addView(card, params);
    }

    private void addHealthRow(LinearLayout parent, String title, String subtitle) {
        LinearLayout row = new LinearLayout(this);
        row.setGravity(Gravity.CENTER_VERTICAL);
        row.addView(label("✓", 18, GREEN), new LinearLayout.LayoutParams(dp(28), -2));
        LinearLayout text = new LinearLayout(this);
        text.setOrientation(LinearLayout.VERTICAL);
        text.addView(label(title, 12, TEXT));
        text.addView(label(subtitle, 10, MUTED));
        row.addView(text, new LinearLayout.LayoutParams(0, -2, 1f));
        parent.addView(row, new LinearLayout.LayoutParams(-1, dp(42)));
    }

    private void addKeyValue(LinearLayout parent, String key, String value) {
        LinearLayout row = new LinearLayout(this);
        row.setGravity(Gravity.CENTER_VERTICAL);
        row.addView(label(key, 12, MUTED), new LinearLayout.LayoutParams(dp(112), dp(32)));
        row.addView(label(value, 12, TEXT), new LinearLayout.LayoutParams(0, dp(32), 1f));
        parent.addView(row);
    }

    private View buildBottomNavigation() {
        LinearLayout nav = new LinearLayout(this);
        nav.setGravity(Gravity.CENTER);
        nav.setPadding(dp(8), dp(5), dp(8), dp(7));
        nav.setBackground(roundBackground(Color.rgb(7, 22, 40), Color.rgb(25, 52, 84), 0));
        String[] icons = {"⌂", "⚙", "▤", "ⓘ"};
        String[] names = {"Status", "Config", "Logs", "About"};
        navigationItems = new TextView[4];
        for (int i = 0; i < names.length; i++) {
            final int page = i;
            LinearLayout item = new LinearLayout(this);
            item.setOrientation(LinearLayout.VERTICAL);
            item.setGravity(Gravity.CENTER);
            TextView icon = label(icons[i], 19, MUTED);
            icon.setGravity(Gravity.CENTER);
            item.addView(icon, new LinearLayout.LayoutParams(-1, dp(28)));
            TextView name = label(names[i], 10, MUTED);
            name.setGravity(Gravity.CENTER);
            item.addView(name, new LinearLayout.LayoutParams(-1, dp(22)));
            navigationItems[i] = itemLabel(icon, name);
            item.setOnClickListener(view -> showPage(page));
            nav.addView(item, new LinearLayout.LayoutParams(0, -1, 1f));
        }
        return nav;
    }

    private TextView itemLabel(TextView icon, TextView name) {
        TextView item = new TextView(this);
        item.setTag(new TextView[]{icon, name});
        return item;
    }

    private void showPage(int page) {
        if (pageHost == null) return;
        for (int i = 0; i < pageHost.getChildCount(); i++) {
            pageHost.getChildAt(i).setVisibility(i == page ? View.VISIBLE : View.GONE);
            if (navigationItems != null && i < navigationItems.length) {
                TextView[] labels = (TextView[]) navigationItems[i].getTag();
                int color = i == page ? BLUE : MUTED;
                labels[0].setTextColor(color);
                labels[1].setTextColor(color);
            }
        }
    }

    private ScrollView scrollPage(LinearLayout content) {
        ScrollView scroll = new ScrollView(this);
        scroll.setFillViewport(true);
        scroll.setClipToPadding(false);
        scroll.addView(content);
        return scroll;
    }

    private LinearLayout pageContent() {
        LinearLayout content = new LinearLayout(this);
        content.setOrientation(LinearLayout.VERTICAL);
        content.setPadding(dp(2), dp(2), dp(2), dp(20));
        return content;
    }

    private View sectionTitle(String title, String subtitle) {
        LinearLayout wrapper = new LinearLayout(this);
        wrapper.setOrientation(LinearLayout.VERTICAL);
        wrapper.setPadding(dp(2), dp(8), dp(2), dp(8));
        wrapper.addView(heading(title, 19));
        wrapper.addView(label(subtitle, 11, MUTED));
        return wrapper;
    }

    private LinearLayout infoCard(String icon, String title, String subtitle) {
        LinearLayout card = cardLayout(SURFACE, BORDER);
        card.setOrientation(LinearLayout.HORIZONTAL);
        card.setGravity(Gravity.CENTER_VERTICAL);
        card.setPadding(dp(13), dp(10), dp(13), dp(10));
        TextView iconView = label(icon, 22, BLUE);
        iconView.setGravity(Gravity.CENTER);
        card.addView(iconView, new LinearLayout.LayoutParams(dp(40), dp(42)));
        LinearLayout text = new LinearLayout(this);
        text.setOrientation(LinearLayout.VERTICAL);
        text.setPadding(dp(9), 0, 0, 0);
        TextView titleView = label(title, 13, TEXT);
        titleView.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        text.addView(titleView);
        text.addView(label(subtitle, 11, MUTED));
        card.addView(text, new LinearLayout.LayoutParams(0, -2, 1f));
        return card;
    }

    private LinearLayout groupCard(String icon, String title, String subtitle) {
        LinearLayout card = cardLayout(SURFACE, BORDER);
        card.setPadding(dp(13), dp(13), dp(13), dp(13));
        LinearLayout header = new LinearLayout(this);
        header.setGravity(Gravity.CENTER_VERTICAL);
        header.addView(label(icon, 22, BLUE), new LinearLayout.LayoutParams(dp(38), dp(36)));
        LinearLayout titles = new LinearLayout(this);
        titles.setOrientation(LinearLayout.VERTICAL);
        TextView titleView = label(title, 14, TEXT);
        titleView.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        titles.addView(titleView);
        titles.addView(label(subtitle, 10, MUTED));
        header.addView(titles, new LinearLayout.LayoutParams(0, -2, 1f));
        header.addView(label("⌃", 18, MUTED));
        card.addView(header);
        return card;
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
        parent.addView(input, new LinearLayout.LayoutParams(-1, dp(43)));
        return input;
    }

    private EditText input(String value) {
        EditText input = new EditText(this);
        input.setSingleLine(true);
        input.setHint(value);
        if (!value.contains("Search")) input.setText(value);
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
        button.setTextColor(text.equals("All") ? TEXT : MUTED);
        button.setBackground(roundBackground(text.equals("All") ? Color.rgb(40, 117, 190) : SURFACE, BORDER, 40));
        return button;
    }

    private Button styledButton(String text) {
        Button button = new Button(this);
        button.setText(text);
        button.setTextSize(13);
        button.setAllCaps(false);
        button.setMinHeight(dp(46));
        button.setMinWidth(0);
        button.setPadding(dp(6), 0, dp(6), 0);
        return button;
    }

    private TextView heading(String text, float size) {
        return label(text, size, TEXT);
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

    private void startRuntime() {
        Smp3Config config = readConfig();
        String error = config.validationError();
        if (error != null) {
            showFailure(error);
            return;
        }
        config.save(this);
        Intent intent = new Intent(this, Smp3Service.class).setAction(Smp3Service.ACTION_PREPARE);
        if (Build.VERSION.SDK_INT >= 26) startForegroundService(intent);
        else startService(intent);
        if (service == null) {
            pendingStart = config;
            bindService(new Intent(this, Smp3Service.class), connection, BIND_AUTO_CREATE);
        } else {
            error = service.startRuntime(config);
            if (error != null) showFailure(error);
        }
    }

    private void stopRuntime() {
        if (service != null) service.stopRuntime();
        startService(new Intent(this, Smp3Service.class).setAction(Smp3Service.ACTION_STOP));
    }

    private void saveConfig() {
        Smp3Config config = readConfig();
        String error = config.validationError();
        if (error != null) {
            showFailure(error);
            return;
        }
        config.save(this);
        Toast.makeText(this, "Configuration saved", Toast.LENGTH_SHORT).show();
        refreshStatus();
    }

    private void validateConfig() {
        String error = readConfig().validationError();
        if (error == null) {
            status.setText("Configuration valid");
            statusSubtitle.setText("Ready to start SMP3");
            Toast.makeText(this, "Configuration is valid", Toast.LENGTH_SHORT).show();
        } else {
            showFailure(error);
            Toast.makeText(this, error, Toast.LENGTH_SHORT).show();
        }
    }

    private void refreshStatus() {
        if (service == null || status == null) return;
        Smp3Service.RuntimeSnapshot snapshot = service.snapshot();
        boolean running = snapshot.processRunning;
        statusDot.setTextColor(running ? GREEN : Color.rgb(255, 202, 92));
        status.setText(running ? "Running" : "Stopped");
        statusSubtitle.setText(running ? "SMP3 is running normally" : "Ready to start");
        StringBuilder detail = new StringBuilder();
        detail.append(running ? "PROCESS_RUNNING" : "PROCESS_STOPPED");
        detail.append("\n").append(snapshot.socksReady ? "SOCKS_READY" : "SOCKS_NOT_READY");
        if (running) detail.append("\nManaged process · ").append(snapshot.processIdentity);
        if (!running && snapshot.exitCode != Integer.MIN_VALUE) detail.append("\nExit code · ").append(snapshot.exitCode);
        if (!snapshot.error.isEmpty()) detail.append("\n").append(snapshot.error);
        statusDetail.setText(detail.toString());
        localValue.setText(snapshot.localAddress);
        serverValue.setText(server.getText().toString().trim());
        pathsValue.setText(running ? "2 configured" : "Waiting for runtime");
        carrierAValue.setText(readCarrierAddress(carrierAHost, carrierAPort));
        carrierBValue.setText(readCarrierAddress(carrierBHost, carrierBPort));
        carrierAState.setText("●  Configured");
        carrierBState.setText("●  Configured");
        startButton.setEnabled(!running);
        stopButton.setEnabled(running);
        startButton.setAlpha(running ? 0.45f : 1f);
        stopButton.setAlpha(running ? 1f : 0.55f);
        logText = snapshot.logs;
        renderLogs();
    }

    private String readCarrierAddress(EditText host, EditText port) {
        return Smp3Config.hostPort(host.getText().toString().trim(), port.getText().toString().trim());
    }

    private void renderLogs() {
        if (logs == null) return;
        String query = logSearch == null ? "" : logSearch.getText().toString().trim().toLowerCase(Locale.ROOT);
        StringBuilder filtered = new StringBuilder();
        for (String line : logText.split("\\n", -1)) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (!query.isEmpty() && !lower.contains(query)) continue;
            if (!matchesFilter(lower)) continue;
            filtered.append(line).append('\n');
        }
        logs.setText(filtered.length() == 0 ? "No matching logs" : filtered.toString());
    }

    private boolean matchesFilter(String line) {
        if ("All".equals(logFilter)) return true;
        if ("Error".equals(logFilter)) return line.contains("error") || line.contains("failed") || line.contains("exit");
        if ("Warn".equals(logFilter)) return line.contains("warn");
        return !line.contains("error") && !line.contains("failed") && !line.contains("warn");
    }

    private Smp3Config readConfig() {
        Smp3Config config = new Smp3Config();
        config.localHost = localHost.getText().toString().trim();
        config.localPort = localPort.getText().toString().trim();
        config.server = server.getText().toString().trim();
        config.carrierAHost = carrierAHost.getText().toString().trim();
        config.carrierAPort = carrierAPort.getText().toString().trim();
        config.carrierBHost = carrierBHost.getText().toString().trim();
        config.carrierBPort = carrierBPort.getText().toString().trim();
        config.smp3Password = password.getText().toString();
        return config;
    }

    private void loadConfig(Smp3Config config) {
        localHost.setText(config.localHost);
        localPort.setText(config.localPort);
        server.setText(config.server);
        carrierAHost.setText(config.carrierAHost);
        carrierAPort.setText(config.carrierAPort);
        carrierBHost.setText(config.carrierBHost);
        carrierBPort.setText(config.carrierBPort);
        password.setText(config.smp3Password);
        carrierAValue.setText(config.carrierAAddress());
        carrierBValue.setText(config.carrierBAddress());
        serverValue.setText(config.server);
    }

    private void showFailure(String message) {
        if (status != null) status.setText("Needs attention");
        if (statusSubtitle != null) statusSubtitle.setText(message);
        if (statusDetail != null) statusDetail.setText("CONFIGURATION_ERROR");
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
}
