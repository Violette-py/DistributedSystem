package api;


/**
* api/NameNodeOperations.java .
* ��IDL-to-Java ������ (����ֲ), �汾 "3.2"����
* ��api.idl
* 2023��11��14�� ���ڶ� ����12ʱ28��14�� CST
*/

public interface NameNodeOperations 
{

  // TODO: complete the interface design
  String open (String filepath, int mode);

  // FIXME: mode?? longi;? int??
  void close (String fileInfo);
} // interface NameNodeOperations
