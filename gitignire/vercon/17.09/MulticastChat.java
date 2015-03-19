/**
 * Created by Администратор on 02.09.14.
 */

package chat;

import java.io.IOException;
import java.net.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;



public class MulticastChat {

    private static final long serialVersionUID = -6539741015936730693L;


    private static final byte INIT_NEGOTIATION      = 1;
    private static final byte NEGOTIATION_RESPONSE  = 2;

    public static final long NEGOTIATIONS_PERIOD = 1 * 60 * 1000L;
    public static final long SLEEP_TIME = 45 * 1000L;
    public static final long NUMBER_TTL = 16 * 1000L;

    private int port = 5050;
    private String group = "225.4.5.6";
    private int ttl = 3;

    private volatile boolean active = true;
    private volatile byte myNumber = 0;
    private volatile boolean primary = false;
    private volatile boolean inProcess = false;
    private volatile long lastNumberGenerationTime = 0;
    private volatile long lastNegotiationTime = 0;



//    private ConcurrentHashMap<String, Boolean> resultsMap = new ConcurrentHashMap<String, Boolean>();
//    private LinkedHashSet<String> stringLinkedHashSet;
//
//    private void getHostAddress () {
//        Enumeration<NetworkInterface> ifs = null;
//        try {
//            ifs = NetworkInterface.getNetworkInterfaces();
//        } catch (SocketException e) {
//            e.printStackTrace();
//        }
//
//        while (ifs.hasMoreElements()) {
//            NetworkInterface i = ifs.nextElement();
//            for(InterfaceAddress addr : i.getInterfaceAddresses()) {
//                stringLinkedHashSet.add(addr.getAddress().getHostAddress());
//            }
//        }
//    }


    public MulticastChat() {
//        getHostAddress();
        new Thread(sender).start();
        new Thread(receiver).start();
    }

    public void send(byte msg[]) {
        try {
            MulticastSocket socket = new MulticastSocket();
            socket.setTimeToLive(ttl);

            DatagramPacket pack = new DatagramPacket(msg, msg.length, InetAddress.getByName(group), port);
            socket.send(pack);

            log("Sent packet: " + msg[0] + ":" + msg[1]);

            socket.close();
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    public synchronized byte getNum(boolean repeat) {
        if ((System.currentTimeMillis() - lastNumberGenerationTime) < NUMBER_TTL && !repeat) {
            return myNumber;
        } else {
            myNumber = (byte) new Random().nextInt(150);
            lastNumberGenerationTime = System.currentTimeMillis();
            return myNumber;
        }
    }

    private Runnable sender = new Runnable() {
        @Override
        public void run() {
            while (active) {
                try {
                    Thread.sleep(SLEEP_TIME);

                    if((System.currentTimeMillis() - lastNegotiationTime) > SLEEP_TIME) {
                        MulticastSocket socket = new MulticastSocket();
                        socket.setTimeToLive(ttl);

//                        if (resultsMap.isEmpty()) {
                        byte num = getNum(false);

                        primary = true;
                        send(new byte[]{INIT_NEGOTIATION, num});

                        log("Starting negotiations with num: " + num);

                        if ((System.currentTimeMillis() - lastNegotiationTime) > NEGOTIATIONS_PERIOD ) initNegotiations();

                        socket.close();
                    }
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    };

    private Runnable receiver = new Runnable() {
        @Override
        public void run() {
            while (active) {
                try {
                    MulticastSocket socket = new MulticastSocket(port);
                    InetAddress address = InetAddress.getByName(group);
                    socket.joinGroup(address);

                    byte[] buf = new byte[2];

                    while(active) {
                        DatagramPacket packet = new DatagramPacket(buf, buf.length);
                        socket.receive(packet);

                        log("Got packet from " + packet.getAddress().getHostName() + ": " + buf[0] + ":" + buf[1] + " is my: " + isMyPacket(packet));

                        if (isMyPacket(packet)) continue;

                        if (buf[1] != myNumber) getNum(true);

                        switch (buf[0]) {
                            case INIT_NEGOTIATION:

                                byte num = getNum(false);
                                if (num < buf[1]) {
                                    primary = false;
                                }

                                send(new byte[]{ NEGOTIATION_RESPONSE, getNum(false)} );

                                // resultsMap.put(packet.getAddress().getHostAddress().toString(), false);
                            case NEGOTIATION_RESPONSE:
                                byte myNum = getNum(false);

                                if (myNum < buf[1]) {
                                    primary = false;
                                }

                                if ((System.currentTimeMillis() - lastNegotiationTime) > NEGOTIATIONS_PERIOD ) initNegotiations();
                                // resultsMap.put(packet.getAddress().getHostAddress().toString(), false);
                        }
                        System.out.println(packet.getAddress().getHostAddress() + " : " + buf[1]);
                    }

                    socket.leaveGroup(address);
                    socket.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    };

    private Runnable defInit = new Runnable() {
        @Override
        public void run() {
            try {
                System.out.println(primary ? "Server: " + InetAddress.getLocalHost().getHostAddress() + " is primary" : "Server not primary");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

//            try {
//                Thread.sleep(SLEEP_TIME);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

            inProcess = false;
        }
    };

    private boolean isMyPacket(DatagramPacket packet) throws IOException {
        if (packet.getAddress().getHostAddress().equals(InetAddress.getLocalHost().getHostAddress()) || packet.getAddress().getHostAddress().startsWith("127.")) {
                return true;
        }
//        Object[] addr = stringLinkedHashSet.toArray();
//        for (int i = addr.length; i>0; i--) {
//            if (addr[i].equals(InetAddress.getLocalHost().getAddress())) {
//                if (addr[i].toString().startsWith("127.")) {
//                    return false;
//                }
//            }
//        }
        return false;
    }

    private void initNegotiations() {
        if (inProcess) return;
        lastNegotiationTime = System.currentTimeMillis();
        inProcess = true;
        new Thread(defInit).start();
//        try {
//            System.out.println("Your Host addr: " + InetAddress.getLocalHost().getHostAddress());  // often returns "127.0.0.1"
//
//            Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
//            for (int i=1; n.hasMoreElements(); i++) {
//                NetworkInterface e = n.nextElement();
//
//                Enumeration<InetAddress> a = e.getInetAddresses();
//                for (; a.hasMoreElements();) {
//                    InetAddress addr = a.nextElement();
//                    if (addr.getHostAddress().startsWith("192.")) System.out.println("  " + addr.getHostAddress() + "   - " + i);
//                }
//            }
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        } catch (SocketException e) {
//            e.printStackTrace();
//        }
    }

    private void log(String msg) {
        System.out.println(new Date() + ": " + msg);
    }

    public static void main(String[] args) {
        new MulticastChat();
    }
}