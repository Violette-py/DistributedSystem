package api;


/**
* api/NameNodeOperations.java .
* ��IDL-to-Java ������ (����ֲ), �汾 "3.2"����
* ��api.idl
* 2023��10��31�� ���ڶ� ����03ʱ53��13�� CST
*/

public interface NameNodeOperations 
{

  // TODO: complete the interface design
  String open (String filepath, int mode);
  void close (String fileInfo);
} // interface NameNodeOperations
