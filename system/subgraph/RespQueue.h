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
 * ��Ӧ���У�����洢����������Ӧ�����������Ӧ������͸����� worker 
 */

#ifndef RESPQUEUE_H_
#define RESPQUEUE_H_

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include "util/conque_p.h"
#include <atomic>
#include <thread>
#include <unistd.h> //for usleep()
using namespace std;

template <class VertexT>
class RespQueue { //part of ReqServer
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    typedef conque_p<VertexT> Buffer; // �����������ͣ����ڴ洢�������̵���Ӧ���
    typedef vector<Buffer> Queue; // �����б��䳤�ȵ��� worker ���������Ӷ���֤һ�� worker ӵ��һ����Ӧ���У�

    /**
     * �����б����� nums_worker �����У�ͨ�� RespQueue.add() ������Ķ��н��и�ֵ
     */
    Queue q;

    /**
     * ���𲻶�ѭ��ȡ�������б� q �еĶ��У�Ȼ�����л��Ľ������Ϣ����ʽ���͸�����Դ
     */
    thread main_thread;

    /**
     * ����Ӧ������л����Ӷ�ת���ɿ��Է��͵���Ϣ
     * @param i �� i �� worker
     * @param m �洢��Ӧ����������㣩���л��������
     */
    void get_msgs(int i, ibinstream & m)
	{
    	if(i == _my_rank) return;
    	Buffer & buf = q[i]; // ȡ���� i �� worker ����Ӧ������У��Ӷ�������Ӧ������л�����Ϣ
		VertexT* temp; //for fetching VertexT items
		while(temp = buf.dequeue()) //fetching till reach list-head
		{
			m << *temp;
		}
	}

    /**
     * ѭ�����������б� q �еĶ��У�Ȼ�󽫶����е��������л���֮��������Ϣ���ݷ��͸���Ӧ������ worker
     */
    void thread_func() //managing requests to tgt_rank
    {
    	int i = 0; //target worker to send ��Ϣ���͵�Ŀ�� worker
    	bool sth_sent = false; //if sth is sent in one round, set it as true ����Ƿ���Ҫ������Ϣ
        // �� m0��m1 ��������Ŀ�ģ���Ϊ���ö���������л��Ĺ��̡�������Ϣ����Ӧ������Ĺ��̵Ĳ���ִ��
        // ���У��� m0 �����ڶ������л����� m1 �����ڷ�����Ϣ����֮�������߽�ɫ������
    	ibinstream* m0 = new ibinstream;
    	ibinstream* m1 = new ibinstream;

        // ��һ�η��Ͷ����б��� 0 �� worker �Ķ�������
        // ����ڽ���ѭ��֮ǰ����ǰ�� 0 �� worker �������л��� m0 �У�Ȼ���ڽ���ѭ��ʱ��һ�߷�����Ϣ��һ�����л��´η��͵Ķ��У����� i+1 �� worker �Ķ��У�
    	thread t(&RespQueue::get_msgs, this, 0, ref(*m0)); //assisting thread �����̣߳����ڽ���Ӧ�����еĶ���������л����Ӷ�ת������Ϣ���з���
    	bool use_m0 = true; //tag for alternating ����Ƿ�ʹ�� m0 �е����ݷ�����Ϣ�����Ϊ true ���� m0 �е���Ϣ��m1 �������л��������򣬷��� m1 �е���Ϣ��m0 �������л���
        
        // ����ѭ���ر��������б� q �����ȡ�����еĶ��У�Ȼ�󽫶����е��������л���������Ϣ���ͣ���Ҫע����ǣ���ǰ��ѭ�������л��Ľ�������´�ѭ��ʱ���ͣ�
        while(global_end_label == false) //otherwise, thread terminates
    	{
			t.join();//m0 or m1 becomes ready to send �������л�������߳��Ѿ����������л���Ϻ�ִ���������Ϣ����
			int j = i+1; // j ���´η�����Ϣ��Ŀ�� worker
			if(j == _num_workers) j = 0; // ���Ѿ�������һ������б� q�����ͷ��ʼ����

            // m0��m1 �������ڷ�����Ϣ���Ӷ�ʹ�÷�����Ϣ�����л����������̲���ִ��
			if(use_m0) //even 
			{
				//use m0, set m1 ��ʹ�� m0 �����ݷ�����Ϣ���� m1 ���л�����
				t = thread(&RespQueue::get_msgs, this, j, ref(*m1)); // �� j �Ŷ��еĶ���������л�����Ϣ��������Ϣ���ݷ����� m1 ��
				// ��Ϊ�ǽ� j �� worker �����еĶ������л��� m1 �У������´����� m1 �����ݷ�����Ϣʱ��Ŀ�� worker Ӧ��Ϊ j �� worker�������ں��潫 j ��ֵ���� i����� i Ҳ��Ŀ�� worker��
                // ���߳� t ���ж������л��Ĺ����У����߳� main_thread �ڷ�����Ϣ�����߲���ִ��
                if(m0->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt ���������Ӧ������͸�����Դ��i �� worker��
					MPI_Send(m0->get_buf(), m0->size(), MPI_CHAR, i, RESP_CHANNEL, MPI_COMM_WORLD);
					delete m0;
					m0 = new ibinstream;
				}
				use_m0 = false; // ��ǰ��ʹ�� m0 �е����ݷ�����Ϣ��m1 �洢���л���������´�ʹ�� m1 ������Ϣ
			}
			else
			{   
				//use m1, set m0 ��ʹ�� m1 �����ݷ�����Ϣ���� m0 ���л�����
				t = thread(&RespQueue::get_msgs, this, j, ref(*m0));
				if(m1->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m1->get_buf(), m1->size(), MPI_CHAR, i, RESP_CHANNEL, MPI_COMM_WORLD);
					//------
					delete m1;
					m1 = new ibinstream;
				}
				use_m0 = true;  // ��ǰ��ʹ�� m1 �е����ݷ�����Ϣ��m0 �洢���л���������´�ʹ�� m0 ������Ϣ
			}
			//------------------------
			i = j; // ��Ϊ j ���´���Ϣ���͵�Ŀ�� worker������ڽ����´�ѭ��ǰ���� j ��ֵ�� i����֤�� i ����Ϣ���͵�Ŀ�� worker
			if(j == 0) // j = 0 ʱ���Ѿ������� worker �Ķ��У��������б� q��������һ�飬���Ѿ�Ϊ���� worker ������һ����Ӧ���
			{
				if(!sth_sent) usleep(WAIT_TIME_WHEN_IDLE); // ���û����Ϣ��Ҫ���ͣ������һ��ʱ��
				else sth_sent = false; // ����Ϣ��Ҫ���ͣ��� sth_sent ����Ϊ false���Ӷ���ʼ��һ����Ϣ�б�
			}
    	}
    	t.join();
		delete m0;
		delete m1;
    }

    RespQueue()
    {
    	q.resize(_num_workers);
    	main_thread = thread(&RespQueue::thread_func, this);
    }

    ~RespQueue()
    {
    	main_thread.join();
    }

    /**
     * ����Ӧ������뵽��Ӧ���� worker ����Ӧ������
     *
     * @param v   �����ȡ�Ķ��㣬����Ӧ���
     * @param tgt ����Դ id ������ǰ worker ����� worker id 
     */
    void add(VertexT * v, int tgt)
    {
    	Buffer & buf = q[tgt];
    	buf.enqueue(v);
    }
};

#endif
