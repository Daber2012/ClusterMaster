
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClusterDiscoveryService {

    public static final byte INIT_NEGOTIATION      = 1;
    public static final byte NEGOTIATION_RESPONSE  = 2;
    public static final byte STATUS_REQUEST        = 3;
    public static final byte STATUS_RESPONSE       = 4;

    public static final int DEFAULT_PACKET_TTL     = 3;
    public static final int DEFAULT_PORT           = 5050;
    public static final String DEFAULT_GROUP       = "225.4.5.6";
	
    public static final long NEGOTIATIONS_PERIOD    = 10 * 60 * 1000L;
    public static final long SLEEP_TIME             = 2 * 60 * 1000L;
    public static final long NUMBER_TTL             = 2 * 60 * 1000L;

    private int port = DEFAULT_PORT;
    private String group = DEFAULT_GROUP;
    private int ttl = DEFAULT_PACKET_TTL;

    private boolean logEnabled = true;

    private volatile boolean active = true;

    private volatile long generatedNumber;

    private volatile boolean primaryServer = false;
    private volatile boolean negotiationsInProcess = false;

    private volatile long lastNumberGenerationTime = 0;
    private volatile long lastNegotiationTime = 0;

    private Random random = new Random();

    private Queue<Runnable> scheduledJobs = new ConcurrentLinkedQueue<Runnable>();

    public ClusterDiscoveryService() {
        new Thread(sender).start();
        new Thread(receiver).start();
        new Thread(addTask).start();
    }

    public boolean isPrimaryServer() {
        return primaryServer;
    }

    public void setPrimaryServer(boolean primaryServer) {
        this.primaryServer = primaryServer;
    }

    public boolean isLogEnabled() {
        return logEnabled;
    }

    public void setLogEnabled(boolean logEnabled) {
        this.logEnabled = logEnabled;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public static long getNegotiationsPeriod() {
        return NEGOTIATIONS_PERIOD;
    }

    public static long getSleepTime() {
        return SLEEP_TIME;
    }

    public static long getNumberTtl() {
        return NUMBER_TTL;
    }

    public void executeOnPrimary(final Runnable command) {
        if (negotiationsInProcess && primaryServer) {
            scheduledJobs.add(command);
        } else {
            if(isPrimaryServer()) command.run();
        }
    }

    private void send(byte msgType, long number) {
        try {
            MulticastSocket socket = new MulticastSocket();
            socket.setTimeToLive(ttl);

            byte[] msg = new byte[9];
            msg[0] = msgType;

            writeNumber(msg, number, 1);

            DatagramPacket pack = new DatagramPacket(msg, msg.length, InetAddress.getByName(group), port);
            socket.send(pack);


            if (msg[0] != STATUS_REQUEST && msg[0] != STATUS_RESPONSE)
                log("Sent packet: " + msg[0] + ":" + getCurrentNum());

            socket.close();
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    public static long readNumber(byte[] buf, int offset) {
        long num = 0;
        for (int i = 0; i < 8; i++) {
            num |= (long) (0xff & buf[offset + i]) << ((7 - i) * 8);
        }
        return num;
    }

    public static void writeNumber(byte[] buf, long number, int offset) {
        for (int i = 0; i < 8; i++) {
            buf[i + offset] = (byte) (number >> ((7 - i) * 8));
        }
    }

    private synchronized long getCurrentNum() {
        if ((System.currentTimeMillis() - lastNumberGenerationTime) > NUMBER_TTL) {
            regenerateNum();
        }

        return generatedNumber;
    }

    private synchronized void regenerateNum() {
        generatedNumber = random.nextLong();
        lastNumberGenerationTime = System.currentTimeMillis();
    }

    private synchronized void reinitNegotiation() {
        regenerateNum();
        initNegotiations();
    }

    private Runnable sender = new Runnable() {
        @Override
        public void run() {
            while (active) {
                try {

                    if ((System.currentTimeMillis() - lastNegotiationTime) >= NEGOTIATIONS_PERIOD) {

                        MulticastSocket socket = new MulticastSocket();
                        socket.setTimeToLive(ttl);

                        primaryServer = true;

                        long currentNumber = getCurrentNum();
                        send(INIT_NEGOTIATION, currentNumber);

                        log("Starting negotiations with num: " + currentNumber);
                        socket.close();

                        initNegotiations();
                    }

                    Thread.sleep(NEGOTIATIONS_PERIOD);

                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    };

    private Runnable receiver = new Runnable() {
        @Override
        public void run() {
            try {
                MulticastSocket socket = new MulticastSocket(port);
                InetAddress address = InetAddress.getByName(group);
                socket.joinGroup(address);

                byte[] buf = new byte[9];

                while(active) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);

                    if (isMyPacket(packet)) continue;

                    long receivedNumber = readNumber(buf, 1);

                    if (buf[0] != 3 && buf[0] != 4)
                        log(packet.getAddress().getHostAddress() + ": " + buf[0] + ":" + receivedNumber);

                    switch (buf[0]) {
                        case INIT_NEGOTIATION:

                            initNegotiations();

                            long currentNum = getCurrentNum();

                            if (currentNum == receivedNumber) {
                                regenerateNum();
                            }

                            send(NEGOTIATION_RESPONSE, currentNum);

                            if (currentNum < receivedNumber) {
                                primaryServer = false;
                            }

                            log(primaryServer ? "yes" : "not" );

                            break;
                        case NEGOTIATION_RESPONSE:

                            if (getCurrentNum() == receivedNumber) {
                                reinitNegotiation();
                                send(NEGOTIATION_RESPONSE, getCurrentNum());
                            }

                            if (getCurrentNum() < receivedNumber)
                                primaryServer = false;

                            log(primaryServer ? "yes" : "not" );

                            break;
                        case STATUS_REQUEST:
                            send(STATUS_RESPONSE, (isPrimaryServer()? 1L : 0L) << 32 | (negotiationsInProcess ? 1 : 0) );
                            break;
                        case STATUS_RESPONSE: break;
                    }
                }

                socket.leaveGroup(address);
                socket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    };

    public Runnable newTask = new Runnable() {
        @Override
        public void run() {
            try {
                log("New task running" + InetAddress.getLocalHost().getHostAddress());
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    };

    private Runnable addTask = new Runnable() {
        @Override
        public void run() {
            for (;;) {
                try {
                    Thread.sleep(5 * 60 *1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                executeOnPrimary(newTask);
            }
        }
    };

    private Runnable defInit = new Runnable() {
        @Override
        public void run() {

            try {
                log(primaryServer ? " --1-- Server: " + InetAddress.getLocalHost().getHostAddress() + " is primary" : " --1-- Server not primary");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                log(primaryServer ? " --2-- Server: " + InetAddress.getLocalHost().getHostAddress() + " is primary" : " --2-- Server not primary");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            if(primaryServer) {
                Runnable r = scheduledJobs.poll();

                while(r != null) {
                    r.run();
                    r = scheduledJobs.poll();
                }
            }

            scheduledJobs.clear();

            negotiationsInProcess = false;
        }
    };

    private boolean isMyPacket(DatagramPacket packet) throws IOException {
        return (packet.getAddress().getHostAddress().equals(InetAddress.getLocalHost().getHostAddress()) || packet.getAddress().getHostAddress().startsWith("127."));
    }

    private void initNegotiations() {
        if (negotiationsInProcess || (System.currentTimeMillis() - lastNegotiationTime) < NUMBER_TTL) return;
        lastNegotiationTime = System.currentTimeMillis();
        negotiationsInProcess = true;
        primaryServer = true;
        new Thread(defInit).start();
    }

    protected void log(String msg) {
		if(logEnabled) System.out.println(new Date() + ": " + msg);
    }

    public static void main(String[] args) {
        new ClusterDiscoveryService();
    }
}