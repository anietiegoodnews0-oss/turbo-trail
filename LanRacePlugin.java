package com.turbotrail.racers;

import android.content.Context;
import android.net.wifi.WifiManager;
import com.getcapacitor.JSArray;
import com.getcapacitor.JSObject;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@CapacitorPlugin(name = "LanRace")
public class LanRacePlugin extends Plugin {
    static final int TCP_PORT = 47890;
    static final int UDP_PORT = 47891;

    static class Peer {
        int id;
        Socket sock;
        PrintWriter out;
    }

    private ServerSocket server;
    private final Map<Integer, Peer> peers = new ConcurrentHashMap<>();
    private int nextId = 1;
    private volatile boolean hosting = false;
    private volatile boolean discovering = false;
    private String roomCode = "";
    private String hostName = "";
    private DatagramSocket recvSock;
    private WifiManager.MulticastLock mlock;
    private final Map<String, JSONObject> rooms = new ConcurrentHashMap<>();
    private final Map<String, Long> seen = new ConcurrentHashMap<>();
    private Socket clientSock;
    private PrintWriter clientOut;

    private void emit(String name, JSObject o) {
        notifyListeners(name, o);
    }

    private List<InetAddress> broadcasts() {
        List<InetAddress> list = new ArrayList<>();
        try {
            for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                for (InterfaceAddress ia : ni.getInterfaceAddresses()) {
                    InetAddress b = ia.getBroadcast();
                    if (b != null) list.add(b);
                }
            }
        } catch (Exception ignored) {
        }
        try {
            list.add(InetAddress.getByName("255.255.255.255"));
        } catch (Exception ignored) {
        }
        return list;
    }

    @PluginMethod
    public void host(PluginCall call) {
        roomCode = call.getString("code", "0000");
        hostName = call.getString("name", "Host");
        try {
            stopHostInternal();
            server = new ServerSocket();
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(TCP_PORT));
            hosting = true;
            new Thread(this::acceptLoop).start();
            new Thread(this::announceLoop).start();
            call.resolve();
        } catch (Exception e) {
            call.reject("Could not host: " + e.getMessage());
        }
    }

    private void acceptLoop() {
        while (hosting) {
            try {
                Socket s = server.accept();
                s.setTcpNoDelay(true);
                final Peer p = new Peer();
                p.id = nextId++;
                p.sock = s;
                p.out = new PrintWriter(s.getOutputStream(), true);
                peers.put(p.id, p);
                JSObject o = new JSObject();
                o.put("id", p.id);
                o.put("event", "joined");
                emit("peer", o);
                new Thread(() -> readLoop(p)).start();
            } catch (IOException e) {
                if (!hosting) break;
            }
        }
    }

    private void readLoop(Peer p) {
        try {
            BufferedReader r = new BufferedReader(new InputStreamReader(p.sock.getInputStream()));
            String line;
            while ((line = r.readLine()) != null) {
                JSObject o = new JSObject();
                o.put("from", p.id);
                o.put("data", line);
                emit("message", o);
            }
        } catch (IOException ignored) {
        }
        peers.remove(p.id);
        try {
            p.sock.close();
        } catch (IOException ignored) {
        }
        JSObject o = new JSObject();
        o.put("id", p.id);
        o.put("event", "left");
        emit("peer", o);
    }

    private void announceLoop() {
        DatagramSocket s = null;
        try {
            s = new DatagramSocket();
            s.setBroadcast(true);
            while (hosting) {
                JSONObject j = new JSONObject();
                j.put("t", "room");
                j.put("code", roomCode);
                j.put("name", hostName);
                j.put("port", TCP_PORT);
                j.put("players", peers.size() + 1);
                byte[] b = j.toString().getBytes("UTF-8");
                for (InetAddress a : broadcasts()) {
                    try {
                        s.send(new DatagramPacket(b, b.length, a, UDP_PORT));
                    } catch (Exception ignored) {
                    }
                }
                Thread.sleep(1000);
            }
        } catch (Exception ignored) {
        }
        if (s != null) s.close();
    }

    @PluginMethod
    public void startDiscovery(PluginCall call) {
        try {
            stopDiscoveryInternal();
            WifiManager wm = (WifiManager) getContext().getApplicationContext().getSystemService(Context.WIFI_SERVICE);
            if (wm != null) {
                mlock = wm.createMulticastLock("lanrace");
                mlock.setReferenceCounted(false);
                mlock.acquire();
            }
            recvSock = new DatagramSocket(null);
            recvSock.setReuseAddress(true);
            recvSock.setBroadcast(true);
            recvSock.bind(new InetSocketAddress(UDP_PORT));
            recvSock.setSoTimeout(1000);
            discovering = true;
            rooms.clear();
            seen.clear();
            new Thread(this::discoverLoop).start();
            call.resolve();
        } catch (Exception e) {
            call.reject("Discovery failed: " + e.getMessage());
        }
    }

    private void discoverLoop() {
        byte[] buf = new byte[1024];
        DatagramSocket s = recvSock;
        while (discovering && s != null && !s.isClosed()) {
            try {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                s.receive(p);
                JSONObject j = new JSONObject(new String(p.getData(), 0, p.getLength(), "UTF-8"));
                if ("room".equals(j.optString("t"))) {
                    j.put("ip", p.getAddress().getHostAddress());
                    String key = j.optString("code") + "@" + j.optString("ip");
                    rooms.put(key, j);
                    seen.put(key, System.currentTimeMillis());
                }
            } catch (SocketTimeoutException ignored) {
            } catch (Exception e) {
                if (!discovering) break;
            }
            long now = System.currentTimeMillis();
            for (String k : new ArrayList<>(seen.keySet())) {
                Long t = seen.get(k);
                if (t != null && now - t > 4000) {
                    seen.remove(k);
                    rooms.remove(k);
                }
            }
            JSArray arr = new JSArray();
            for (JSONObject j : rooms.values()) arr.put(j);
            JSObject o = new JSObject();
            o.put("rooms", arr);
            emit("rooms", o);
        }
    }

    @PluginMethod
    public void join(final PluginCall call) {
        final String ip = call.getString("ip");
        final int port = call.getInt("port", TCP_PORT);
        new Thread(() -> {
            boolean connected = false;
            try {
                leaveInternal();
                Socket s = new Socket();
                s.connect(new InetSocketAddress(ip, port), 4000);
                s.setTcpNoDelay(true);
                clientSock = s;
                clientOut = new PrintWriter(s.getOutputStream(), true);
                connected = true;
                call.resolve();
                BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream()));
                String line;
                while ((line = r.readLine()) != null) {
                    JSObject o = new JSObject();
                    o.put("from", 0);
                    o.put("data", line);
                    emit("message", o);
                }
            } catch (Exception e) {
                if (!connected) {
                    call.reject("Could not join: " + e.getMessage());
                }
            }
            JSObject o = new JSObject();
            o.put("id", 0);
            o.put("event", "left");
            emit("peer", o);
        }).start();
    }

    @PluginMethod
    public void send(PluginCall call) {
        PrintWriter o = clientOut;
        if (o != null) o.println(call.getString("data", ""));
        call.resolve();
    }

    @PluginMethod
    public void broadcast(PluginCall call) {
        String d = call.getString("data", "");
        for (Peer p : peers.values()) p.out.println(d);
        call.resolve();
    }

    @PluginMethod
    public void sendTo(PluginCall call) {
        Peer p = peers.get(call.getInt("id", -1));
        if (p != null) p.out.println(call.getString("data", ""));
        call.resolve();
    }

    @PluginMethod
    public void stopHost(PluginCall call) {
        stopHostInternal();
        call.resolve();
    }

    @PluginMethod
    public void stopDiscovery(PluginCall call) {
        stopDiscoveryInternal();
        call.resolve();
    }

    @PluginMethod
    public void leave(PluginCall call) {
        leaveInternal();
        call.resolve();
    }

    private void stopHostInternal() {
        hosting = false;
        try {
            if (server != null) server.close();
        } catch (Exception ignored) {
        }
        server = null;
        for (Peer p : peers.values()) {
            try {
                p.sock.close();
            } catch (Exception ignored) {
            }
        }
        peers.clear();
        nextId = 1;
    }

    private void stopDiscoveryInternal() {
        discovering = false;
        try {
            if (recvSock != null) recvSock.close();
        } catch (Exception ignored) {
        }
        recvSock = null;
        try {
            if (mlock != null && mlock.isHeld()) mlock.release();
        } catch (Exception ignored) {
        }
    }

    private void leaveInternal() {
        try {
            if (clientSock != null) clientSock.close();
        } catch (Exception ignored) {
        }
        clientSock = null;
        clientOut = null;
    }
                    }
