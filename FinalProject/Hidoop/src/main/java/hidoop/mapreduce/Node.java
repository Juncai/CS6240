package hidoop.mapreduce;

import hidoop.util.Consts;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
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
