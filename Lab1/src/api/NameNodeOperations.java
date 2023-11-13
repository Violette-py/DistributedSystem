package api;


/**
* api/NameNodeOperations.java .
* 由IDL-to-Java 编译器 (可移植), 版本 "3.2"生成
* 从api.idl
* 2023年11月13日 星期一 下午03时58分46秒 CST
*/

public interface NameNodeOperations 
{

  // TODO: complete the interface design
  String open (String filepath, int mode);

  // FIXME: mode?? longi;? int??
  void close (String fileInfo);
} // interface NameNodeOperations
