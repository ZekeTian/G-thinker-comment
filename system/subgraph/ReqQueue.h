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
 * ������У��洢��Ҫ���͵����󣬲�����Զ�� worker ��������
 */


#ifndef REQQUEUE_H_
#define REQQUEUE_H_

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include "util/conque.h"
#include <atomic>
#include <thread>
#include <unistd.h> //for usleep()
using namespace std;

template <class VertexT>
class ReqQueue {
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    /**
     * ������У��洢�� worker ������ȡ�Ķ���� id
     */
    typedef conque<KeyT> Buffer; // ������У��洢�� worker ��������ȡ�Ķ���� id
    
    /**
     * ��������б�����
     */
    typedef vector<Buffer> Queue;

    /**
     * ��������б��洢�Ÿ��� worker ��������С�ÿ�� worker ����һ��������У���˸��б��й��� nums_worker ������
     * �б��е� 0 �����д洢���ǣ���ǰ worker �� 0 �� worker ����Ķ���� id��
     * ��ǰworker --����--> i��worker����ǰ worker ��� i �� worker �����������б��е� i �����д洢�����������������Ҫ��ȡ�Ķ��� id
     */
    Queue q; // ��������б�

    /**
     * ReqQueue �е����̣߳����������󣬻�ȡԶ�� worker �Ķ���
     */
    thread main_thread;

    /**
     * ����Ҫ����Ķ���� id �������л������浽�� m �У��Ӷ����ں��淢����Ϣ��ȡ��Ӧ�Ķ���
     */
    void get_msgs(int i, ibinstream & m)
	{
    	if(i == _my_rank) return;
    	Buffer & buf = q[i]; // ȡ����ǰ worker ��� i �� worker ����Ķ�������
		// ������� i �� worker ���͵�������һ�� buf �д洢�Ķ��� id ���л��� m ����
        KeyT temp; //for fetching KeyT items
        while(req_counter[i] < REQUEST_BOUND && buf.dequeue(temp))
        { //fetching till reach list-head
            m << temp;
            req_counter[i]++; // �� i �� worker �������������
            if(m.size() > MAX_BATCH_SIZE) break; //cut at batch size boundary һ���ﵽһ������Ķ������������ޣ���˴����󲻻��ټ������� buf �е����ݣ������´�ѭ���� i �� worker ʱ�������� buf ��ʣ������ݣ�
        }
	}

    /**
     * ѭ��������������б� q������ȡ���������ݣ�Ȼ�����л������У����ͨ����Ϣ�����������ݣ���ȡԶ�� worker �Ķ��㡣
     * �߼��� RespQueue �����ƣ�Ҳ��һ�߽������������л������У�һ�߷�����������
     */
    void thread_func() //managing requests to tgt_rank
    {
    	int i = 0; //target worker to send ��Ϣ���͵�Ŀ�� worker�������������ݵ� worker��
    	bool sth_sent = false; //if sth is sent in one round, set it as true
    	ibinstream* m0 = new ibinstream;
    	ibinstream* m1 = new ibinstream;
    	//m0 and m1 are alternating
    	thread t(&ReqQueue::get_msgs, this, 0, ref(*m0)); //assisting thread
    	bool use_m0 = true; //tag for alternating
        clock_t last_tick = clock();

        // ����ѭ������������� q ������ȡ���������ݣ�Ȼ�����л������У����ͨ����Ϣ������������
    	while(global_end_label == false) //otherwise, thread terminates
    	{
    		t.join(); //m0 or m1 becomes ready to send
    		int j = i+1; //  j ���´η�����Ϣ��Ŀ�� worker
    		if(j == _num_workers) j = 0;
    		if(use_m0) //even
    		{
    			//use m0, set m1
    			t = thread(&ReqQueue::get_msgs, this, j, ref(*m1));
				if(m0->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m0->get_buf(), m0->size(), MPI_CHAR, i, REQ_CHANNEL, MPI_COMM_WORLD);
					//------
					delete m0;
					m0 = new ibinstream;
				}
				use_m0 = false;
    		}
    		else //odd
    		{
    			//use m1, set m0
    			t = thread(&ReqQueue::get_msgs, this, j, ref(*m0));
				if(m1->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m1->get_buf(), m1->size(), MPI_CHAR, i, REQ_CHANNEL, MPI_COMM_WORLD);
					//------
					delete m1;
					m1 = new ibinstream;
				}
				use_m0 = true;
    		}
    		//------------------------
    		i = j;
    		if(j == 0)
    		{
    			if(!sth_sent) usleep(WAIT_TIME_WHEN_IDLE);
    			/*else{
                    sth_sent = false;
                    clock_t time_passed = clock() - last_tick; //processing time
                    clock_t gap = polling_ticks - time_passed; //remaining time before next polling
                    if(gap > 0) usleep(gap * 1000000 / CLOCKS_PER_SEC);
                }
                last_tick = clock();*/
    		}
    	}
    	t.join();
    	delete m0;
    	delete m1;
    }

    ReqQueue()
    {
    	q.resize(_num_workers);
    	main_thread = thread(&ReqQueue::thread_func, this);
    }

    ~ReqQueue()
    {
    	main_thread.join();
    }

    HashT hash;

    void add(KeyT vid)
    {
    	int tgt = hash(vid);
    	Buffer & buf = q[tgt];
    	buf.enqueue(vid);
    }
};

#endif
