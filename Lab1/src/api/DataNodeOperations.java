package api;


/**
* api/DataNodeOperations.java .
* ��IDL-to-Java ������ (����ֲ), �汾 "3.2"����
* ��api.idl
* 2023��11��14�� ���ڶ� ����10ʱ20��41�� CST
*/

public interface DataNodeOperations 
{
  byte[] read (int block_id);
  int append (int block_id, byte[] bytes);
  int randomBlockId ();
} // interface DataNodeOperations
