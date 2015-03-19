/**
 * Created by Volodymir on 22.09.14.
 */

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClusterReferee {

    static class ServiceEntry {
        String ip;
        boolean primary;
        boolean inProcess;

        public ServiceEntry (String ip, boolean prim, boolean inProc) {
            this.ip = ip;
            this.primary = prim;
            this.inProcess = inProc;
        }
    }


    public volatile boolean active = true;

    private CopyOnWriteArrayList<ServiceEntry> serversIps = new CopyOnWriteArrayList<ServiceEntry>();

    ClusterReferee() {
        new Thread(sender).start();
        new Thread(receiver).start();
    }

    private void requestStatuses() {
        try {
            MulticastSocket socket = new MulticastSocket();

            socket.setTimeToLive(ClusterDiscoveryService.DEFAULT_PACKET_TTL);
            byte[] msg = new byte[] { ClusterDiscoveryService.STATUS_REQUEST };

            DatagramPacket pack = new DatagramPacket(msg, msg.length,
                    InetAddress.getByName(ClusterDiscoveryService.DEFAULT_GROUP), ClusterDiscoveryService.DEFAULT_PORT);

            socket.send(pack);

            socket.close();
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    Runnable sender = new Runnable() {
        @Override
        public void run() {
            while (active) {
                
                requestStatuses();   
                
                try { Thread.sleep(30000); } catch (InterruptedException e) { }

                if (!serversIps.isEmpty()) {
                    boolean hasUndecided = false;
                    int count = 0;

                    for (ServiceEntry se : serversIps) {
                        if(se.inProcess) {
                            hasUndecided = true;
                            break;
                        }

                        if (se.primary) count++;
                    }

                    if (count > 1 && !hasUndecided) log("-----------------------------------------------------------------CONFLICT---------------------");
                    if (count == 0 && !hasUndecided) log("-----------------------------------------------------------------NO PRIMARY---------------------");
                    serversIps.clear();
                }


            }
        }
    };

    Runnable receiver = new Runnable() {
        @Override
        public void run() {
            while (active) {
                try {
                    MulticastSocket socket = new MulticastSocket(ClusterDiscoveryService.DEFAULT_PORT);
                    InetAddress address = InetAddress.getByName(ClusterDiscoveryService.DEFAULT_GROUP);
                    socket.joinGroup(address);

                    byte[] buf = new byte[9];

                    while(active) {
                        DatagramPacket packet = new DatagramPacket(buf, buf.length);
                        socket.receive(packet);

                        if (buf[0] != ClusterDiscoveryService.STATUS_RESPONSE) continue;

                        long data = ClusterDiscoveryService.readNumber(buf, 1);
                        boolean primary = (data >> 32) > 0;
                        boolean inProcess = (0xffffffffL & data) > 0;

                        log("Server " + packet.getAddress().getHostAddress() + " is ");
                        log(primary ? " primary" : "not primary ");
                        log(inProcess ? " and on process" : " finally");

                        serversIps.add(new ServiceEntry(packet.getAddress().getHostAddress(), primary, inProcess));
                    }

                    socket.leaveGroup(address);
                    socket.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    };

    protected void log(String msg) {
        System.out.println(new Date() + ": " + msg);
    }

    public static void main (String args[]) {
        new ClusterReferee();
    }
}