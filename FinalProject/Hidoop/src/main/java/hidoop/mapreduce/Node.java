package hidoop.mapreduce;

import hidoop.util.Consts;

/**
 * Created by jon on 4/21/16.
 */
public class Node {
    public int index;
    public Consts.NodeStatus status;
    public String ip;

    public Node(int index, String ip) {
        this.index = index;
        this.ip = ip;
        status = Consts.NodeStatus.OFFLINE;
    }
}
