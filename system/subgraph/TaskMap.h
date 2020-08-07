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
 * ���� Map �����������������С��������� Map�����һ���һ���̹߳�����
 * ��Ҫע����ǣ��տ�ʼ����������к͹������� map ��Ϊ�գ�Ȼ��������Ҫ��ȡԶ�̶���ʱ����ô������ͻ�Ž��Ĺ������� map �У�
 * �ȸù��������ȡ��������Ҫ�Ķ������ݺ󣬻Ὣ������ӹ������� map ���Ƶ�������������С�����������£�
 *  ��Ҫ��ȡԶ�� worker ��������� --�����Ͷ�����ȡ���󣬴��ڹ���״̬��--> �Ž��������� map --����ȡ��������Ҫ��Զ�̶��㣬���ھ���״̬��--> �Ž��������������
 */

#ifndef TASKMAP_H_
#define TASKMAP_H_

#include <atomic>
#include "util/conque_p.h"
#include "util/global.h"
#include <iostream>
#include "util/conmap2t.h"

using namespace std;

template <class TaskT>
class TaskMap {
public:
    // ���� task ���У�����ִ��������������������ִ�������̶߳���ʱֹͣ
	conque_p<TaskT> task_buf; //for keeping ready tasks
	//- pushed by Comper
	//- popped by RespServer

	typedef conmap2t<long long, TaskT *> TMap; // TMap[task_id] = task ���� map ���ͣ�key Ϊ task_id��value Ϊ��Ӧ�� task

    /**
     * �������� map ������Ϊ��<���� id, ����>�������洢������ִ�����������񣬱���˵�ڵȴ�Զ�̶��㷵�ص��������Ӧ�����е� Task Table T_task
     */
	TMap task_map; //for keeping pending tasks
	//- added by Comper
	//- removed by RespServer

    /**
     * ��һ�������˳���
     */
	unsigned int seqno; //sequence number for tasks, go back to 0 when used up �����˳��ţ��� 0 ��ʼ

    /**
     * �����̵߳��̺߳�
     */
	int thread_rank; //associated with which thread? �̺߳ţ���¼�������߳�

    /**
     * ���������������ֵ = ����������д�С + �������� map ��С
     * ������ add2map ʱ�����µĹ���������룬������������ 1
     * ������ get() ʱ���Ӿ������������ȡ������������������ 1
     *    �����仯���̣��¹���������뵽�������� map �У����� +1�� ----> ������������������������� ----> �Ӿ���������ȡ���������� -1��
     * ע�⣺�ӹ������� map ���������������������У�ֻ�������״̬�����仯����������û�б仯����� size ���ᷢ���仯
     */
	atomic<int> size; // ���������������ֵ = ����������д�С + �������� map ��С
	//updated by Comper, read by Worker's status_sync
	//> ++ by add2map(.)
	//> -- by get(.)

    /**
     * ȡ����һ������� id �����������˳��� seqno ������������ Task.pull_all() ��ʹ�á�
     */
	long long peek_next_taskID() //not take it
	{
		long long id = thread_rank;
		id = (id << 48); //first 16-bit is thread_id ǰ 16 λ��Ϊ�̺߳ţ��� 48 λ��Ϊ����š��̺߳����� 48 λ���൱���ǣ��̺߳� * (2^48)��Ȼ�����������ϼ��������˳��ż��ɵõ������ id��
        // �� i ���̵߳��̺߳����� 48 λ��Ľ����� i+1 ���߳����ƵĽ��֮������ 2^48 ����������߳�֮������㹻�������洢����š�
        // ʵ���ϣ��߳��������� 2^16 ������ÿ���߳��ֿ����� 2^48 �����������㹻��
        // ��������� id �ܹ��� 64 λ��ǰ 16 λ���̺߳ţ��� 48 λ�Ǹ������ڵ�ǰ�߳��ڲ���һ��˳��ţ�seqno�����������������Ϊ�����γɸ�����������
        // ������ƿ��Ժܷ���ش�������л�ȡ�������Ӧ���̺߳ţ���ֻ��Ҫ����������� 48 λ��ɵõ��̺߳�
		return (id + seqno);
	}

    /**
     * ��ȡ��һ������� id �����������˳��� seqno �����������������ʱ���øú�����
     */
	long long get_next_taskID() //take it
	{
		long long id = thread_rank;
		id = (id << 48); //first 16-bit is thread_id
		id += seqno;
		seqno++;
		return id;
	}

	TaskMap() //need to set thread_rank right after the object creation
	{
		seqno = 0;
		size = 0;
	}

	TaskMap(int thread_id)
	{
		thread_rank = thread_id;
		seqno = 0;
		size = 0;
	}

    /**
     * ��������ӵ��������� map �У��� adjCache.lock_and_get �б����ã���Ҫ�ǽ�����Զ�̶����������뵽�������� map �У�
     * 
     */
	//called by Comper, need to provide counter = task.pull_all(.)
	//only called if counter > 0
	void add2map(TaskT * task)
	{
		size++;
        // ����һ������ź󣬽����������������� map ��
		//add to task_map
		long long tid = get_next_taskID(); // ����һ�������
		conmap2t_bucket<long long, TaskT *> & bucket = task_map.get_bucket(tid);  // ����������� map ��
		bucket.lock();
		bucket.insert(tid, task);
		bucket.unlock();
	}//no need to add "v -> tasks" track, should've been handled by lock&get(tid) -> request(tid) �� adjCache.lock_and_get �����У�pcache.request �Ὣ�����붥�����

    /**
     * �� RespServer ��RespServer.thread_func�����ã����ڸ��� task �� counter�����ܻ���¹������� map����
     * ��Ϊ RespServer �Ѿ����յ��˸�������Ҫ��һ��Զ�̶������ݣ���ô����� counter Ӧ�����������������Ѿ���ȡ�����صĶ��������� 1����
     * ������������Ҫ��Զ�̶����Ѿ�ȫ����ȡ������ô�ͻ������� task_map���������� Map�����ƶ� task_buf������������У��С���Ϊ��ʱ�������Ѿ���������������Ҫ�ٱ�����
     *
     * @param task_id   ���� id
     */
	//called by RespServer
	//- counter--
	//- if task is ready, move it from "task_map" to "task_buf"
	void update(long long task_id)
	{
        // �� task_map ��ȡ�� task_id ����
		conmap2t_bucket<long long, TaskT *> & bucket = task_map.get_bucket(task_id);
		bucket.lock();
		hash_map<long long, TaskT *> & kvmap = bucket.get_map();
		auto it = kvmap.find(task_id);
		assert(it != kvmap.end()); //#DEBUG# to make sure key is found
		TaskT * task = it->second;

        // �������Ӧ�� met_counter �� 1���������������Ҫ�Ķ��������Ѿ�ȫ����ȡ�����򽫸�����ӹ��� map ���Ƶ�����������
		task->met_counter++;
		if(task->met_counter == task->req_size())
		{
			task_buf.enqueue(task);
			kvmap.erase(it);
		}
		bucket.unlock();
	}

    /**
     * ���ؾ�����������һ�����������������Ϊ�գ����� NULL��
     */
	TaskT* get() //get the next ready-task in "task_buf", returns NULL if no more
	{
		//note:
		//called by Comper after inserting each task to "task_map"
		//Comper will fetch as many as possible (call get() repeatedly), till NULL is returned

		TaskT* ret = task_buf.dequeue();
		if(ret != NULL) size--;
		return ret;
	}
};

#endif /* TASKMAP_H_ */
