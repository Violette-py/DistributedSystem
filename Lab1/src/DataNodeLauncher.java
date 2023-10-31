import api.DataNode;
import api.DataNodeHelper;
import impl.DataNodeImpl;
import org.omg.CORBA.ORB;
import org.omg.CORBA.ORBPackage.InvalidName;
import org.omg.CosNaming.NameComponent;
import org.omg.CosNaming.NamingContextExt;
import org.omg.CosNaming.NamingContextExtHelper;
import org.omg.CosNaming.NamingContextPackage.CannotProceed;
import org.omg.CosNaming.NamingContextPackage.NotFound;
import org.omg.PortableServer.POA;
import org.omg.PortableServer.POAHelper;
import org.omg.PortableServer.POAManagerPackage.AdapterInactive;
import org.omg.PortableServer.POAPackage.ServantNotActive;
import org.omg.PortableServer.POAPackage.WrongPolicy;

import java.util.Properties;

public class DataNodeLauncher {

    public static void main(String[] args) {

        try {

            Properties properties = new Properties();
            properties.put("org.omg.CORBA.ORBInitialHost", "127.0.0.1");  // ORB IP
            properties.put("org.omg.CORBA.ORBInitialPort", "1050");       // ORB port

            // init ORB object
            ORB orb = ORB.init(args, properties);

            // get RootPOA activate POAManager
            POA rootpoa = POAHelper.narrow(orb.resolve_initial_references("RootPOA"));
            rootpoa.the_POAManager().activate();

            // new a object
            DataNodeImpl dataNodeServant = new DataNodeImpl();

            // export
            org.omg.CORBA.Object ref = rootpoa.servant_to_reference(dataNodeServant);
            DataNode href = DataNodeHelper.narrow(ref);

            // Naming context
            org.omg.CORBA.Object objRef = orb.resolve_initial_references("NameService");
            NamingContextExt ncRef = NamingContextExtHelper.narrow(objRef);

            // bind to Naming
            NameComponent[] path = ncRef.to_name("DataNode");
            ncRef.rebind(path, href);

            System.out.println("DataNode is ready and waiting...");

            // waiting
            orb.run();
        } catch (WrongPolicy | InvalidName | ServantNotActive | AdapterInactive |
                 org.omg.CosNaming.NamingContextPackage.InvalidName | CannotProceed | NotFound e) {
            throw new RuntimeException(e);
        }

    }
}