import api.DataNode;
import api.DataNodeHelper;
import impl.DataNodeImpl;
import org.omg.CORBA.ORB;
import org.omg.CORBA.ORBPackage.InvalidName;
import org.omg.CosNaming.NameComponent;
import org.omg.CosNaming.NamingContextExt;
import org.omg.CosNaming.NamingContextExtHelper;
import org.omg.PortableServer.POA;
import org.omg.PortableServer.POAHelper;
import utils.Constants;

import java.util.Properties;

public class DataNodeLauncher {

//    private static int counter = 0; // 用于 DataNode id自增，与 Client侧 和 DataNodeImpl内 id自增 都对应

    public static void main(String[] args) {

        try {

            int orbInitialPort = -1;
            String orbInitialHost = null;
            int dataNodeId = -1;

            // 解析命令行参数
            for (int i = 0; i < args.length; i++) {
//                System.out.println("arg" + i + " : " + args[i]);
                switch (args[i]) {
                    case "-ORBInitialPort":
                        if (i < args.length - 1) {
                            orbInitialPort = Integer.parseInt(args[i + 1]);
                            i++; // 跳过下一个参数
                        } else {
                            // 处理缺少参数的情况
                            System.err.println("-ORBInitialPort 需要指定端口号");
                            return;
                        }
                        break;
                    case "-ORBInitialHost":
                        if (i < args.length - 1) {
                            orbInitialHost = args[i + 1];
                            i++; // 跳过下一个参数
                        } else {
                            // 处理缺少参数的情况
                            System.err.println("-ORBInitialHost 需要指定主机名");
                            return;
                        }
                        break;
                    case "-DataNodeID":
                        if (i < args.length - 1) {
                            dataNodeId = Integer.parseInt(args[i + 1]);
                            i++; // 跳过下一个参数
                        } else {
                            // 处理缺少参数的情况
                            System.err.println("-DataNodeID 需要指定DataNode ID");
                            return;
                        }
                        break;
                    default:
                        // 处理未知参数
                        System.err.println("未知参数: " + args[i]);
                        return;
                }
            }

            // 检查必需参数
            if (orbInitialPort == -1 || orbInitialHost == null || dataNodeId == -1) {
                System.err.println("缺少必要的参数");
                return;
            }

            // 打印参数值，可以根据需要将这些参数传递给 DataNode 的启动逻辑
            System.out.println("ORBInitialPort: " + orbInitialPort);
            System.out.println("ORBInitialHost: " + orbInitialHost);
            System.out.println("dataNodeID: " + dataNodeId);

            /* ------------------------------------------------------------------------ */

            Properties properties = new Properties();
            properties.put("org.omg.CORBA.ORBInitialHost", "127.0.0.1");  // ORB IP
            properties.put("org.omg.CORBA.ORBInitialPort", "1050");       // ORB port

            // init ORB object
            ORB orb = ORB.init(args, properties);

            // get RootPOA activate POAManager
            POA rootpoa = POAHelper.narrow(orb.resolve_initial_references("RootPOA"));
            rootpoa.the_POAManager().activate();

            // new a object
            DataNodeImpl dataNodeServant = new DataNodeImpl(dataNodeId);
//            DataNodeImpl dataNodeServant = new DataNodeImpl();

            // export
            org.omg.CORBA.Object ref = rootpoa.servant_to_reference(dataNodeServant);
            DataNode href = DataNodeHelper.narrow(ref);

            // Naming context
            org.omg.CORBA.Object objRef = orb.resolve_initial_references("NameService");
            NamingContextExt ncRef = NamingContextExtHelper.narrow(objRef);

            // bind to Naming
            NameComponent[] path = ncRef.to_name("DataNode" + dataNodeId);
//            NameComponent[] path = ncRef.to_name("DataNode" + Constants.DATANODE_ID);

            System.out.println("DataNode " + dataNodeId + " is ready and waiting...");
//            System.out.println("DataNode " + Constants.DATANODE_ID + " is ready and waiting...");

//            Constants.DATANODE_ID++;
            ncRef.rebind(path, href);


            // waiting
            orb.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}