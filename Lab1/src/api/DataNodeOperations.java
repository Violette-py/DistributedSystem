package api;


/**
* api/DataNodeOperations.java .
* ��IDL-to-Java ������ (����ֲ), �汾 "3.2"����
* ��api.idl
* 2023��10��31�� ���ڶ� ����03ʱ53��13�� CST
*/

public interface DataNodeOperations 
{
  byte[] read (int block_id);
  void append (int block_id, byte[] bytes);
  int randomBlockId ();
} // interface DataNodeOperations
