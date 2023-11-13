package api;


/**
* api/byteArrayHelper.java .
* 由IDL-to-Java 编译器 (可移植), 版本 "3.2"生成
* 从api.idl
* 2023年11月14日 星期二 上午12时28分14秒 CST
*/

abstract public class byteArrayHelper
{
  private static String  _id = "IDL:api/byteArray:1.0";

  public static void insert (org.omg.CORBA.Any a, byte[] that)
  {
    org.omg.CORBA.portable.OutputStream out = a.create_output_stream ();
    a.type (type ());
    write (out, that);
    a.read_value (out.create_input_stream (), type ());
  }

  public static byte[] extract (org.omg.CORBA.Any a)
  {
    return read (a.create_input_stream ());
  }

  private static org.omg.CORBA.TypeCode __typeCode = null;
  synchronized public static org.omg.CORBA.TypeCode type ()
  {
    if (__typeCode == null)
    {
      __typeCode = org.omg.CORBA.ORB.init ().get_primitive_tc (org.omg.CORBA.TCKind.tk_octet);
      __typeCode = org.omg.CORBA.ORB.init ().create_array_tc ((int)(4 * 1024), __typeCode );
      __typeCode = org.omg.CORBA.ORB.init ().create_alias_tc (api.byteArrayHelper.id (), "byteArray", __typeCode);
    }
    return __typeCode;
  }

  public static String id ()
  {
    return _id;
  }

  public static byte[] read (org.omg.CORBA.portable.InputStream istream)
  {
    byte value[] = null;
    value = new byte[(int)(4 * 1024)];
    for (int _o0 = 0;_o0 < ((int)(4 * 1024)); ++_o0)
    {
      value[_o0] = istream.read_octet ();
    }
    return value;
  }

  public static void write (org.omg.CORBA.portable.OutputStream ostream, byte[] value)
  {
    if (value.length != ((int)(4 * 1024)))
      throw new org.omg.CORBA.MARSHAL (0, org.omg.CORBA.CompletionStatus.COMPLETED_MAYBE);
    for (int _i0 = 0;_i0 < ((int)(4 * 1024)); ++_i0)
    {
      ostream.write_octet (value[_i0]);
    }
  }

}
