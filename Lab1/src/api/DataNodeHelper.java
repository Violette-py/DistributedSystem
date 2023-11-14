package api;


/**
* api/DataNodeHelper.java .
* 由IDL-to-Java 编译器 (可移植), 版本 "3.2"生成
* 从api.idl
* 2023年11月14日 星期二 下午10时20分41秒 CST
*/

abstract public class DataNodeHelper
{
  private static String  _id = "IDL:api/DataNode:1.0";

  public static void insert (org.omg.CORBA.Any a, api.DataNode that)
  {
    org.omg.CORBA.portable.OutputStream out = a.create_output_stream ();
    a.type (type ());
    write (out, that);
    a.read_value (out.create_input_stream (), type ());
  }

  public static api.DataNode extract (org.omg.CORBA.Any a)
  {
    return read (a.create_input_stream ());
  }

  private static org.omg.CORBA.TypeCode __typeCode = null;
  synchronized public static org.omg.CORBA.TypeCode type ()
  {
    if (__typeCode == null)
    {
      __typeCode = org.omg.CORBA.ORB.init ().create_interface_tc (api.DataNodeHelper.id (), "DataNode");
    }
    return __typeCode;
  }

  public static String id ()
  {
    return _id;
  }

  public static api.DataNode read (org.omg.CORBA.portable.InputStream istream)
  {
    return narrow (istream.read_Object (_DataNodeStub.class));
  }

  public static void write (org.omg.CORBA.portable.OutputStream ostream, api.DataNode value)
  {
    ostream.write_Object ((org.omg.CORBA.Object) value);
  }

  public static api.DataNode narrow (org.omg.CORBA.Object obj)
  {
    if (obj == null)
      return null;
    else if (obj instanceof api.DataNode)
      return (api.DataNode)obj;
    else if (!obj._is_a (id ()))
      throw new org.omg.CORBA.BAD_PARAM ();
    else
    {
      org.omg.CORBA.portable.Delegate delegate = ((org.omg.CORBA.portable.ObjectImpl)obj)._get_delegate ();
      api._DataNodeStub stub = new api._DataNodeStub ();
      stub._set_delegate(delegate);
      return stub;
    }
  }

  public static api.DataNode unchecked_narrow (org.omg.CORBA.Object obj)
  {
    if (obj == null)
      return null;
    else if (obj instanceof api.DataNode)
      return (api.DataNode)obj;
    else
    {
      org.omg.CORBA.portable.Delegate delegate = ((org.omg.CORBA.portable.ObjectImpl)obj)._get_delegate ();
      api._DataNodeStub stub = new api._DataNodeStub ();
      stub._set_delegate(delegate);
      return stub;
    }
  }

}
