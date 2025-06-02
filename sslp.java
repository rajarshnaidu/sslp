import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.SSLSocket;

public class SSLPoke {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java SSLPoke <host> <port>");
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        try {
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            SSLSocket socket = (SSLSocket) factory.createSocket(host, port);
            socket.startHandshake();
            socket.close();
            System.out.println("Successfully connected");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
