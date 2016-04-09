package hidoop.conf;

import hidoop.fs.Path;
import hidoop.mapreduce.Mapper;
import hidoop.mapreduce.Partitioner;
import hidoop.mapreduce.Reducer;
import hidoop.util.Consts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jon on 4/6/16.
 */
public class Configuration {
    public boolean isLocalMode;
    public String jobName;
    public Class mapperClass;
    public Class reducerClass;
    public Class partitionerClass;
    public Class outputKeyClass;
    public Class outputValueClass;
    public Class mapOutputKeyClass;
    public Class mapOutputValueClass;
    public Path inputPath;
    public Path outputPath;
    public int reducerNumber;
    private List<String> slaveIpList;
    private String masterIp;
    private int masterPort;
    private int slavePort;
    private int slaveNum;


    public Configuration() throws IOException {
        isLocalMode = true;
        // TODO load local config file
        // slave IPs
        BufferedReader br = new BufferedReader(new FileReader(Consts.IP_LIST_PATH));
        slaveIpList = new ArrayList<String>();
        String line = null;
        while ((line = br.readLine()) != null) {
            slaveIpList.add(line);
            System.out.println("adding ip to slave iplist: " + line);
        }
        br.close();
        slaveNum = slaveIpList.size();
        reducerNumber = slaveNum;

        // mode: local/ec2
        // master port
        // slave port
        // master ip
        br = new BufferedReader(new FileReader(Consts.CONFIG_PATH));
        isLocalMode = br.readLine().equals(Consts.LOCAL_MODE);
        masterPort = Integer.parseInt(br.readLine());
        slavePort = Integer.parseInt(br.readLine());
        masterIp = br.readLine();

        System.out.println("Master port: " + masterPort);
        System.out.println("Slave port: " + slavePort);
        System.out.println("Master ip: " + masterIp);
    }

    public void setJobName(String jname) {
        this.jobName = jname;
    }

    public void setMapperClass(Class<? extends Mapper> cls) {
        this.mapperClass = cls;
    }

    public void setReducerClass(Class<? extends Reducer> cls) {
        this.reducerClass = cls;
    }

    public void setPartitionerClass(Class<? extends Partitioner> cls) {
        this.partitionerClass = cls;
    }

    public void setOutputKeyClass(Class<?> cls) {
        this.outputKeyClass = cls;
    }

    public void setOutputValueClass(Class<?> cls) {
        this.outputValueClass = cls;
    }

    public void setMapOutputKeyClass(Class<?> cls) {
        this.mapOutputKeyClass = cls;
    }

    public void setMapOutputValueClass(Class<?> cls) {
        this.mapOutputValueClass = cls;
    }

    public void setInputPath(Path p) {
        this.inputPath = p;
    }

    public void setOutputPath(Path p) {
        this.outputPath = p;
    }

    public void set(String name, String value) {
        // do nothing;
//        set(name, value, null);
    }

}
