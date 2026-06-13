package com.example;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;

// MedWebSocketServer to serwer WebSocket, który łączy backend Storm z dashboardem w przeglądarce.
public class MedWebSocketServer extends WebSocketServer {

    private static MedWebSocketServer instance;

    public MedWebSocketServer(int port) {
        super(new InetSocketAddress(port));
    }

    // tworzy jedną instancję serwera (Singleton) i uruchamia go w osobnym wątku
    public static void initialize(int port) {
        instance = new MedWebSocketServer(port);
        instance.start();
    }

    // zwraca instancję serwera, żeby MedicalAlertBolt mógł wysyłać dane do przeglądarki
    public static MedWebSocketServer getInstance() {
        return instance;
    }

    // rozsyła komunikat JSON do wszystkich podłączonych przeglądarek
    public void broadcastAlert(String text) {
        for (WebSocket conn : getConnections()) {
            conn.send(text);
        }
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("[WebSocket] Przeglądarka połączona: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("[WebSocket] Przeglądarka rozłączona.");
    }

    @Override
    public void onMessage(WebSocket conn, String message) {}

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("[WebSocket] Błąd: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("[WebSocket] Serwer wizualizacji wystartował na porcie " + getPort());
    }
}
