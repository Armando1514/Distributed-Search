import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {


    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    // IF zookeeper doesn't hear back from a client in 3000ms will consider the client disconnected or dead.
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String ELECTION_NAMESPACE = "/election";

    private String currentZnodeName;

    public static void main(String [] args) throws IOException, InterruptedException, KeeperException {

        LeaderElection leaderElection = new LeaderElection();

        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper, exiting application.");

    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath =
                zooKeeper.create(znodePrefix, new byte[]{},
                         ZooDefs.Ids.OPEN_ACL_UNSAFE,
                         CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name " + znodeFullPath);

        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");

    }

    public void reelectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZNodeName = "";

        while(predecessorStat == null) {
            List<String> children =
                    zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);

            String smallestChild = children.get(0);
            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                return;
            } else {
                System.out.println("I am not the leader, the leader is:" + smallestChild + "is the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZNodeName = children.get(predecessorIndex);

                // can be that at moment I call this, the ZNode is gone because the node disconnected.
                // so it returns null -> I need to take care of this failure scenario of race condiiton.
                // so we retry with while(predecessorStat == null)
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZNodeName, this);
            }

            System.out.println("Watching znode" + predecessorZNodeName);
        }
    }

    public void run() throws InterruptedException{


        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,
                SESSION_TIMEOUT,
                this);
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Client successfully connected to ZooKeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Client disconnected from Zookeeper Event, so notify all the clients");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
                break;
        }
    }
}
