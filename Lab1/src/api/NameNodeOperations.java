package api;


/**
* api/NameNodeOperations.java .
* ��IDL-to-Java ������ (����ֲ), �汾 "3.2"����
* ��api.idl
* 2023��11��13�� ����һ ����03ʱ58��46�� CST
*/

public interface NameNodeOperations 
{

  // TODO: complete the interface design
  String open (String filepath, int mode);

  // FIXME: mode?? longi;? int??
  void close (String fileInfo);
} // interface NameNodeOperations
