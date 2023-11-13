package api;


/**
* api/DataNodePOA.java .
* 由IDL-to-Java 编译器 (可移植), 版本 "3.2"生成
* 从api.idl
* 2023年11月13日 星期一 下午03时58分46秒 CST
*/

public abstract class DataNodePOA extends org.omg.PortableServer.Servant
 implements api.DataNodeOperations, org.omg.CORBA.portable.InvokeHandler
{

  // Constructors

  private static java.util.Hashtable _methods = new java.util.Hashtable ();
  static
  {
    _methods.put ("read", new java.lang.Integer (0));
    _methods.put ("append", new java.lang.Integer (1));
    _methods.put ("randomBlockId", new java.lang.Integer (2));
  }

  public org.omg.CORBA.portable.OutputStream _invoke (String $method,
                                org.omg.CORBA.portable.InputStream in,
                                org.omg.CORBA.portable.ResponseHandler $rh)
  {
    org.omg.CORBA.portable.OutputStream out = null;
    java.lang.Integer __method = (java.lang.Integer)_methods.get ($method);
    if (__method == null)
      throw new org.omg.CORBA.BAD_OPERATION (0, org.omg.CORBA.CompletionStatus.COMPLETED_MAYBE);

    switch (__method.intValue ())
    {
       case 0:  // api/DataNode/read
       {
         int block_id = in.read_long ();
         byte $result[] = null;
         $result = this.read (block_id);
         out = $rh.createReply();
         api.byteArrayHelper.write (out, $result);
         break;
       }

       case 1:  // api/DataNode/append
       {
         int block_id = in.read_long ();
         byte bytes[] = api.byteArrayHelper.read (in);
         this.append (block_id, bytes);
         out = $rh.createReply();
         break;
       }

       case 2:  // api/DataNode/randomBlockId
       {
         int $result = (int)0;
         $result = this.randomBlockId ();
         out = $rh.createReply();
         out.write_long ($result);
         break;
       }

       default:
         throw new org.omg.CORBA.BAD_OPERATION (0, org.omg.CORBA.CompletionStatus.COMPLETED_MAYBE);
    }

    return out;
  } // _invoke

  // Type-specific CORBA::Object operations
  private static String[] __ids = {
    "IDL:api/DataNode:1.0"};

  public String[] _all_interfaces (org.omg.PortableServer.POA poa, byte[] objectId)
  {
    return (String[])__ids.clone ();
  }

  public DataNode _this() 
  {
    return DataNodeHelper.narrow(
    super._this_object());
  }

  public DataNode _this(org.omg.CORBA.ORB orb) 
  {
    return DataNodeHelper.narrow(
    super._this_object(orb));
  }


} // class DataNodePOA
