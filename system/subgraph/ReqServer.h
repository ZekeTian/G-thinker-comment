//########################################################################
//## Copyright 2018 Da Yan http://www.cs.uab.edu/yanda
//##
//## Licensed under the Apache License, Version 2.0 (the "License");
//## you may not use this file except in compliance with the License.
//## You may obtain a copy of the License at
//##
//## //http://www.apache.org/licenses/LICENSE-2.0
//##
//## Unless required by applicable law or agreed to in writing, software
//## distributed under the License is distributed on an "AS IS" BASIS,
//## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//## See the License for the specific language governing permissions and
//## limitations under the License.
//########################################################################

/**
 * ������������������������ڱ��ض����б� local_table �в�����Ҫ��ȡ�Ķ��㣬�����ؽ����
 * �����е��̻߳᲻�ϵ��ظ����У�ֱ��û���µ�����������Ϣ�����û���µ�������Ϣ������̻߳����ߣ��ȴ��µ���Ϣ��
 */

//this is the server of key-value store
#ifndef REQSERVER_H_
#define REQSERVER_H_

//it receives batches of reqs, looks up local_table for vertex objects, and responds
//this is repeated till no msg-batch is probed, in which case it sleeps for "WAIT_TIME_WHEN_IDLE" usec and probe again

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include <unistd.h> //for usleep()
#include <thread>
#include "RespQueue.h"
using namespace std;

template <class VertexT>
class ReqServer {
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;
	typedef hash_map<KeyT, VertexT*> VTable;

	VTable & local_table;
    /**
     * ��Ӧ���У�����洢����������Ӧ�����
     * �ö��ж����з�װ�� num_workers �����У�ÿ�����ж�Ӧһ�� worker����ͬʱ��ά��һ���߳����ڽ���Ӧ���ݷ��ظ������ worker��
     */
	RespQueue<VertexT> q_resp; //will create _num_workers responding threads

    /**
     * ��������������̣߳����ڽ�����Ϣ��
     */
	thread main_thread;


    /**
     * ��ȡ��Ϣ buf �еĶ��� id���������з����л��õ������ id������ȡ��Ӧ�Ķ��㣬��������Ž���Ӧ������
     * @param buf   ��Ϣ����
     * @param size  ��Ϣ��С
     * @param src   ��ϢԴ��������Դ����ǰ worker ��������� worker �� id�� src-wroker --��������--> ��ǰ worker��src-worker Ϊ����Դ��src Ϊ src-worker �� id��
     */
	//*//unsafe version, users need to guarantee that adj-list vertex-item must exist
	//faster
	void thread_func(char * buf, int size, int src)
	{
		obinstream m(buf, size);
		KeyT vid;
		while(m.end() == false)
		{
			m >> vid; // ��ȡ��Ϣ�ж���� id���������з����л��õ������ id�� ���Ӷ��ڶ��� map ��ͨ�� id ��ȡ��Ӧ�Ķ���
			VertexT * v = local_table[vid];
			q_resp.add(local_table[vid], src); // ����Ӧ������뵽 RespQueue �е����� worker ��Ӧ�����У��Ӷ����շ��ظ������ worker
		}
	}
	//*/

	/*//safe version
	void thread_func(char * buf, int size, int src)
	{
		obinstream m(buf, size);
		KeyT vid;
		while(m.end() == false)
		{
			m >> vid;
			auto it = local_table.find(vid);
			if(it == local_table.end())
			{
				cout<<_my_rank<<": [ERROR] Vertex "<<vid<<" does not exist but is requested"<<endl;
				MPI_Abort(MPI_COMM_WORLD, -1);
			}
			q_resp.add(it->second, src);
		}
	}
	//*/

    /**
     * �ڼ���������ͼ���������� worker �ı��ض����б�󣬻ᴴ�� ReqServer ���Ӷ�������һ���߳�ִ�� run ����
     */
    void run() //called after graph is loaded and local_table is set
    {
    	bool first = true; // ����Ƿ��ǵ�һ�ν��� while ѭ����
    	thread t;
    	//------
        // ���ϵ�̽����Ϣ�������ϵؼ���Ƿ��з�����ǰ worker ���������������Ҫ������Ӧ�� worker
    	while(global_end_label == false) //otherwise, thread terminates ����Ҫ worker �����������������Ҫ��������
    	{
    		int has_msg;
    		MPI_Status status;
    		MPI_Iprobe(MPI_ANY_SOURCE, REQ_CHANNEL, MPI_COMM_WORLD, &has_msg, &status); // ̽����յ���Ϣ
            // MPI_Iprobe �Ƿ������͵ģ������Ƿ�̽�⵽��Ϣ���������ء����̽�⵽��Ϣ���� has_msg Ϊ true������Ϊ false��

    		if(!has_msg) usleep(WAIT_TIME_WHEN_IDLE); // û����Ϣʱ�����������߳� WAIT_TIME_WHEN_IDLE ΢�루Ĭ�� 100��
    		else
    		{   // ����Ϣ���������� worker ������ʱ���������󡣼�ȡ������Ķ��㣬������Щ���������Ӧ���У�RespQueue����
    			int size;
    			MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes) ��ȡ���յ�����Ϣ��С
    			char * buf = new char[size]; //space for receiving this msg-batch, space will be released by obinstream in thread_func(.) ���ڴ洢���յ�����Ϣ
    			MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE); // ������Ϣ
    			// �ж��Ƿ��ǵ�һ�ν���ѭ��������ǵ�һ�ν���ѭ�������߳� t δ����������Ҫ join������ ����Ҫ join �߳�
                if(!first) t.join(); //wait for previous CPU op to finish; t can be extended to a vector of threads later if necessary

                // ����һ���µ��̣߳������ȡ��Ϣ buf �еĶ��� id��Ȼ���ȡ��Ӧ���㣬����������뵽��Ӧ������
    			t = thread(&ReqServer<VertexT>::thread_func, this, buf, size, status.MPI_SOURCE); //insert to q_resp[status.MPI_SOURCE]
    			first = false;
    		}
    	}
    	if(!first) t.join();
    }

    /**
     * ��ʼ�� local_table ������������һ���߳�
     */
    ReqServer(VTable & loc_table) : local_table(loc_table) //get local_table from Worker ð�ź���� ��local_table(loc_table)�� �ǶԳ�Ա���� local_table �ĳ�ʼ��
    {
    	main_thread = thread(&ReqServer<VertexT>::run, this);
    }

    ~ReqServer()
    {
    	main_thread.join();
    }
};

#endif
