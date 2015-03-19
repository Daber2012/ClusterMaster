/**
 * Created by Volodymir on 22.09.14.
 */

package sdf;

import sdf.ServerNegotiation;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;

public class NegotiationsServiceImpl {

    static class ServiceEntry {
        String ip;
        boolean prim;
        boolean onProc;

        public ServiceEntry (String ip, boolean prim, boolean onProc) {
            this.ip = ip;
            this.prim = prim;
            this.onProc = onProc;
        }
    }


    public volatile boolean active = true;

    private CopyOnWriteArrayList<ServiceEntry> serversIps = new CopyOnWriteArrayList<ServiceEntry>();

    NegotiationsServiceImpl() {
        new Thread(sender).start();
        new Thread(receiver).start();
    }

    private void requestStatuses() {
        try {
            MulticastSocket socket = new MulticastSocket();

            socket.setTimeToLive(ServerNegotiation.DEFAULT_PACKET_TTL);
            byte[] msg = new byte[] { ServerNegotiation.STATUS_REQUEST };

            DatagramPacket pack = new DatagramPacket(msg, msg.length,
                    InetAddress.getByName(ServerNegotiation.DEFAULT_GROUP), ServerNegotiation.DEFAULT_PORT);

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
                        if(se.onProc) {
                            hasUndecided = true;
                            break;
                        } 

                        if (se.prim) count++;
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
                    MulticastSocket socket = new MulticastSocket(ServerNegotiation.DEFAULT_PORT);
                    InetAddress address = InetAddress.getByName(ServerNegotiation.DEFAULT_GROUP);
                    socket.joinGroup(address);

                    byte[] buf = new byte[3];

                    while(active) {
                        DatagramPacket packet = new DatagramPacket(buf, buf.length);
                        socket.receive(packet);

                        if (buf[0] != ServerNegotiation.STATUS_RESPONSE) continue;

                        log("Server " + packet.getAddress().getHostAddress() + " is ");
                        log((buf[1] == 1) ? " primary" : "not primary ");
                        log((buf[2] == 1) ? " and on process" : " finally");

                        serversIps.add(new ServiceEntry(packet.getAddress().getHostAddress(), buf[1] == 1, buf[2] == 1));
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
        new NegotiationsServiceImpl();
    }
}