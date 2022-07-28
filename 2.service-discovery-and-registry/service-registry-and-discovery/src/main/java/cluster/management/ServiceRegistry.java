package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE="/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZNode = null;
    private List<String> allServiceAddresses = null;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        this.currentZNode = zooKeeper.create(
                REGISTRY_ZNODE + "/n_",
                metadata.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL
        );

        System.out.println("Registered to service registry");
    }
    private void createServiceRegistryZnode() {

        try {
            if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) {

                // there could be race condition if two nodes enters here at the same time
                // solved by zooKeeper by allowing one call to create, in case there will be two, the second one throw an  exception.
                zooKeeper.create(REGISTRY_ZNODE,
                        new byte[]{},
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void registerForUpdates(){
        try{
            updateAddresses();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws InterruptedException, KeeperException {
        if(allServiceAddresses == null){
            updateAddresses();
        }
        return allServiceAddresses;
    }

    public void unregisterFromCluster() throws InterruptedException, KeeperException {
        if(currentZNode != null && zooKeeper.exists(currentZNode, false) != null){
            zooKeeper.delete(currentZNode, -1);
        }
    }

    // this entire update will happen atomically because the method is synchronized
    private synchronized void updateAddresses() throws InterruptedException, KeeperException {
        List<String> workerZNodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);

        List<String> addresses = new ArrayList<>(workerZNodes.size());

        for (String workerZNode : workerZNodes) {
            String workerZNodeFullPath = REGISTRY_ZNODE + "/" + workerZNode;
            Stat stat = zooKeeper.exists(workerZNodeFullPath, false);
            if(stat == null){
                continue;
            }

            byte[] addressBytes = zooKeeper.getData(workerZNodeFullPath, false, stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);

        System.out.println("The cluster addresses are: " + this.allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

        try {
            updateAddresses();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }
}
