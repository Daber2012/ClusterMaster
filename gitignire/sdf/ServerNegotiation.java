/**
 * Created by Volodymir on 02.09.14.
 */

package sdf;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.net.*;
import java.util.*;



public class ServerNegotiation {

    public static final byte INIT_NEGOTIATION      = 1;
    public static final byte NEGOTIATION_RESPONSE  = 2;
    public static final byte STATUS_REQUEST        = 3;
    public static final byte STATUS_RESPONSE       = 4;

    public static final int DEFAULT_PACKET_TTL     = 3;
    public static final int DEFAULT_PORT           = 5050;
    public static final String DEFAULT_GROUP       = "225.4.5.6";
	
    public static final long NEGOTIATIONS_PERIOD    = 1 * 60 * 1000L;
    public static final long SLEEP_TIME             = 20 * 1000L;
    public static final long NUMBER_TTL             = 20 * 1000L;

    private int port = DEFAULT_PORT;
    private String group = DEFAULT_GROUP;
    private int ttl = DEFAULT_PACKET_TTL;

    private boolean logEnabled = true;

    private volatile boolean active = true;

    private volatile byte[] generatedNumber = new byte[6];
    private volatile boolean primaryStat = false;
    private volatile boolean processStat = false;
    private volatile long lastNumberGenerationTime = 0;
    private volatile long lastNegotiationTime = 0;


    public ServerNegotiation() {
        new Thread(sender).start();
        new Thread(receiver).start();
    }

    public boolean isPrimaryStat() {
        return primaryStat;
    }

    public void setPrimaryStat(boolean primaryStat) {
        this.primaryStat = primaryStat;
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
        if (primaryStat) {
            command.run();
        }
    }

    private void send(byte msgType, byte[] arr) {
        try {
            MulticastSocket socket = new MulticastSocket();
            socket.setTimeToLive(ttl);

            byte[] msg = new byte[arr.length + 1];
            msg[0] = msgType;
            System.arraycopy(arr, 0, msg, 1, arr.length);

            DatagramPacket pack = new DatagramPacket(msg, msg.length, InetAddress.getByName(group), port);
            socket.send(pack);

            if (msg[0] != 3 && msg[0] != 4)
                log("Sent packet: " + msg[0] + ":" + Arrays.toString(msg));

            socket.close();
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    private synchronized byte[] getNum() {
        if ((System.currentTimeMillis() - lastNumberGenerationTime) > NUMBER_TTL) {
            regenerateNum();
        }

        return generatedNumber;
    }

    private synchronized void regenerateNum() {
        new Random().nextBytes(generatedNumber);
        lastNumberGenerationTime = System.currentTimeMillis();
    }

    private synchronized void reinitNegotiation() {
        regenerateNum();
        initNegotiations();
    }

    private int compareBuf (byte[] one, byte[] two) {
        BigInteger intOne = new BigInteger(one);
        BigInteger intTwo = new BigInteger(two);
        return intOne.compareTo(intTwo);
    }

    private Runnable sender = new Runnable() {
        @Override
        public void run() {
            while (active) {
                try {

                    if ((System.currentTimeMillis() - lastNegotiationTime) >= NEGOTIATIONS_PERIOD) {

                        MulticastSocket socket = new MulticastSocket();
                        socket.setTimeToLive(ttl);

                        primaryStat = true;

                        byte[] num = getNum();
                        send(INIT_NEGOTIATION, num);

                        log("Starting negotiations with num: " + Arrays.toString(num));
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

                byte[] buf = new byte[7];

                while(active) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);

                    if (isMyPacket(packet)) continue;

                    byte[] bufNum = Arrays.copyOfRange(buf, 1, buf.length);

                    if (buf[0] != 3 && buf[0] != 4)
                        log(packet.getAddress().getHostAddress() + ": " + buf[0] + ":" + buf[1]);

                    switch (buf[0]) {
                        case INIT_NEGOTIATION:

                            initNegotiations();

                            if (getNum()[0] == buf[1])
                                regenerateNum();

                            send(NEGOTIATION_RESPONSE, getNum());

                            if (compareBuf(getNum(), bufNum) < 0)
                                primaryStat = false;

                            log(primaryStat ? "yes" : "not" );

                            //initNegotiations();

                            break;
                        case NEGOTIATION_RESPONSE:

                            if (compareBuf(getNum(), bufNum) == 0) {
                                reinitNegotiation();
                                send(NEGOTIATION_RESPONSE, getNum());
                            }

                            if (compareBuf(getNum(), bufNum) < 0)
                                primaryStat = false;

                            log(primaryStat ? "yes" : "not" );

                            break;
                        case STATUS_REQUEST:
                            send(STATUS_RESPONSE, new byte[] { (byte)(isPrimaryStat()? 1 : 0), (byte)(processStat ? 1 : 0) });
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

    private Runnable defInit = new Runnable() {
        @Override
        public void run() {

            try {
                log(primaryStat ? " --1-- Server: " + InetAddress.getLocalHost().getHostAddress() + " is primary" : " --1-- Server not primary");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                log(primaryStat ? " --2-- Server: " + InetAddress.getLocalHost().getHostAddress() + " is primary" : " --2-- Server not primary");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            processStat = false;
        }
    };

    private boolean isMyPacket(DatagramPacket packet) throws IOException {
        return (packet.getAddress().getHostAddress().equals(InetAddress.getLocalHost().getHostAddress()) || packet.getAddress().getHostAddress().startsWith("127."));
    }

    private void initNegotiations() {
        if (processStat || (System.currentTimeMillis() - lastNegotiationTime) < NUMBER_TTL) return;
        lastNegotiationTime = System.currentTimeMillis();
        processStat = true;
        primaryStat = true;
        new Thread(defInit).start();
    }

    protected void log(String msg) {
		if(logEnabled) System.out.println(new Date() + ": " + msg);
    }

    public static void main(String[] args) {
        new ServerNegotiation();
    }
}