import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZookeeperExample {

    public static void doTest() {
        //the version is not supported when using java8
        String host = System.getenv("host");
        String port = System.getenv("port");
        String url = host + ":" + port;
//        String url="127.0.0.1:2181";
        int timeout = 15000;
        System.out.println("开始获取zookeeper连接...");
        try (
            ZooKeeper zooKeeper = new ZooKeeper(url, timeout,new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    // 发生变更的节点路径
                    String path = watchedEvent.getPath();
                    System.out.println("path:" + path);

                    // 通知状态
                    Event.KeeperState state = watchedEvent.getState();
                    System.out.println("KeeperState:" + state);

                    // 事件类型
                    Event.EventType type = watchedEvent.getType();
                    System.out.println("EventType:" + type);
                }
            })){
            System.out.println("连接成功！");
            zooKeeper.create("/abc", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Stat stat = new Stat();
            byte[] data = zooKeeper.getData("/abc", false, stat);
            System.out.println(new String(data));
            System.out.println(stat.getDataLength());
            zooKeeper.getData("/abc",
                    watchedEvent -> {
                        System.out.println("path:" + watchedEvent.getPath());
                        System.out.println("KeeperState:" + watchedEvent.getState());
                        System.out.println("EventType:" + watchedEvent.getType());
                    },
                    null);
            zooKeeper.setData("/abc", "456".getBytes(), -1);
            zooKeeper.setData("/abc", "789".getBytes(), -1);
            Thread.sleep(1000);
            zooKeeper.delete("/abc", -1);
        } catch (IOException | InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }


    }
}
