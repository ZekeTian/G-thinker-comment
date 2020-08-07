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
 *  ����һ�� Comper �ᴦ���� task 
 */

#ifndef TASK_H_
#define TASK_H_

#include "Subgraph.h"
#include "adjCache.h"
#include "util/serialization.h"
#include "util/ioser.h"
#include "Comper.h"
#include "TaskMap.h"

using namespace std;

template <class VertexT, class ContextT = char>
class Task {
public:
	typedef VertexT VertexType;
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;
	typedef ContextT ContextType;

    typedef Task<VertexT, ContextT> TaskT;
	typedef Subgraph<VertexT> SubgraphT;

	typedef AdjCache<TaskT> CTable;
	typedef hash_map<KeyT, VertexT*> VTable;

	typedef TaskMap<TaskT> TaskMapT;


	SubgraphT subG;
	ContextT context;
	HashT hash;

	//internally used:
    /**
     * ��һ�ֵ�������Ҫ��ȡ�Ķ���� id
     */
	vector<KeyT> to_pull; //vertices to be pulled for use in next round ��һ�ֵ�������Ҫ��ȡ�Ķ���� id
	//- to_pull needs to be swapped to old_to_pull, before calling compute(.) that writes to_pull
	//- unlock vertices in old_to_pull

    /**
     * �洢��ȡ���Ķ��㣬��Щ���������ֵ�����ʹ�á�������е� i ��������Զ�̶��㣬��Ҫͨ��������Ϣ��ȡ������ frontier_vertexes �е� i ��λ��Ϊ NULL
     */
	vector<VertexT *> frontier_vertexes; //remote vertices are replaced with NULL

    /**
     * �Ѿ���ȡ�����صĶ������������ڴ���ȡ�Ķ����У��� met_counter �������Ѿ���ȡ�����أ�
     */
	atomic<int> met_counter; //how many requested vertices are at local, updated by comper/RespServer, not for use by users (system only)

	Task(){}

	Task(const Task & o)
	{
		//defined so that "met_counter" is no longer a problem when using vector<TaskT>
		subG = o.subG;
		context = o.context;
		hash = o.hash;
		to_pull = o.to_pull;
	}

	inline size_t req_size()
	{
		return frontier_vertexes.size();
	}

	/*//deprecated, now Task has no UDF, all UDFs are moved to Comper
	//will be set by Comper before calling compute(), so that add_task() can access comper's add_task()
	virtual bool compute(SubgraphT & g, ContextType & context, vector<VertexType *> & frontier) = 0;
	*/

	//to be used by users in UDF compute(.)
	void pull(KeyT id){
		to_pull.push_back(id);
	}

    /**
     * ��ȡ���㡣
     * �����������Ҫ��Զ�� worker ����ȡ���㣨���Ѿ�����Ӧ�л�ȡ���˶������ݣ����Ǹ�����ǰ�������ܼ����������򷵻� false��
     * �����������Ҫ�Ķ����Ѿ�ȫ����ȡ�����أ��򷵻� true�����Լ�����һ�ֵ�����
     */
	//after task.compute(.) returns, process "to_pull" to:
	//set "frontier_vertexes"
	bool pull_all(thread_counter & counter, TaskMapT & taskmap) //returns whether "no need to wait for remote-pulling"
	{//called by Comper, giving its thread_counter and thread_id
		long long task_id = taskmap.peek_next_taskID(); 
		CTable & vcache = *(CTable *)global_vcache; // ���ػ����
		VTable & ltable = *(VTable *)global_local_table; // ���ض����
		met_counter = 0;
		bool remote_detected = false; //two cases: whether add2map(.) has been called or not ��� task_id �����Ƿ��Ѿ�������true ��ʾ�������Ѿ�����false ��ʾδ����ͬʱҲ���Ա�ʾ�Ƿ���Ҫ����Զ������
        // �����Ҫ�� task_id ������������� add2map���������������� map �У���� task_id ������֮ǰ����Զ�̶���ʱ�Ѿ�����������Ҫ���� 
		int size = to_pull.size();
		frontier_vertexes.resize(size);

        // ��һ��ȡ����
		for(int i=0; i<size; i++)
		{
			KeyT key= to_pull[i];
			if(hash(key) == _my_rank) //in local-table �ڱ��ض����б���
			{
				frontier_vertexes[i] = ltable[key];
				met_counter++;
			}
			else //remote Զ�̶��㣨�����������һ���ǣ���Զ�̶����Ѿ�����ȡ���������뵽������У�����һ���ǣ���Զ�̶���δ����ȡ������Ҫ����Զ����ȡ��
			{
                // ��ȡԶ�̶���ʱ�����жϸ������Ƿ���Ҫ��������������Ѿ���������Ҫ�ٽ������
				if(remote_detected) //no need to call add2map(.) again ����Ҫ�ٴε��� add2map��������Ҫ�ٴν� task_id �������
				{
                    // task_id �����Ѿ�����
					frontier_vertexes[i] = vcache.lock_and_get(key, counter, task_id); // ����� lock_and_get �����в������ add2map 
					if(frontier_vertexes[i] != NULL) met_counter++; // �� i �������Ѿ���ȡ�����أ��� met_counter �� 1
				}
				else
				{
                    // task_id ����δ������
					frontier_vertexes[i] = vcache.lock_and_get(key, counter, task_id,
											taskmap, this); // ����ڻ������ҵ���Ӧ���㣬��ֱ�ӷ��ض��㣻������Ҫ��Զ�� worker ��ȡ���㣬����� add2map
					if(frontier_vertexes[i] != NULL) met_counter++;
					else //add2map(.) is called
					{
                        // ������ add2map()���� task_id ���������Ҫ�� remote_detected ��Ϊ true����Ǹ����񱻹���
                        // �������´���ȡԶ�̶���ʱ�����жϸ������Ƿ��Ѿ�����������������Ѿ����ڹ���״̬������Ҫ���� add2map 
						remote_detected = true;
					}
				}
			}
		}

        // �ж��Ƿ���ҪԶ�����������ҪԶ����������Ҫ�ж��Ƿ���Ҫ���������״̬
		if(remote_detected)
		{
            // ��Ҫ����Զ�������ȡ����
			//so far, all pull reqs are processed, and pending resps could've arrived (not wakening the task)
			//------
            // �ӹ������� map �л�ȡ task_id �����ж��Ƿ�ɹ���ȡ��
			conmap2t_bucket<long long, TaskT *> & bucket = taskmap.task_map.get_bucket(task_id); // �������� map
			bucket.lock();
			hash_map<long long, TaskT *> & kvmap = bucket.get_map();
			auto it = kvmap.find(task_id);

			if(it != kvmap.end())
			{
                // �������� map �д��� task_id ����
				if(met_counter == req_size())  // ������ж��㶼�Ѿ���ȡ�������������Ҫ�ӹ���״̬ת���ɾ���״̬
				{//ready for task move, delete
					kvmap.erase(it); // �� task �ӹ��������б���ɾ���������뵽�������������
					taskmap.task_buf.enqueue(this); 
				}
				//else, RespServer will do the move ����Ҫ�Ķ���δ��ȡ�꣬RespServer ����ժȡԶ�̶���
			}
			//else, RespServer has already did the move �ڹ������� map ��δ��ȡ�� task_id �������п��� RespServer ���Ѿ������� task_id ��������Ҫ��Զ�̶������ݣ�Ȼ�󽫸�����ӹ������� map ���Ƶ�������������У���ˣ��ڹ������� map ��δ�ҵ�������
			bucket.unlock();
			return false;//either has pending resps, or all resps are received but the task is now in task_buf (to be processed, but not this time) 
            // Զ������δ���� �� Զ�������Ѿ����ص��ǵ�ǰ�����ھ�����������У���������ִ�У�
		}
		else return true; //all v-local, continue to run the task for another iteration ���ж���������ȫ����ȡ���أ����Լ�����һ�ֵ���
	}

    /**
     * �������ʱ���ô˺�����������������ʹ�õ�Զ�̶��㣨���»��������Щ�����״̬��
     */
	void unlock_all()
	{
        // ��ȡ��ǰ worker �Ļ�������»�����еĶ���״̬����Ҫ�Ƕ���ļ���������ʹ�ö��������������
		CTable & vcache = *(CTable *)global_vcache;

        // ������ǰ������ʹ�õ����ж��㣬�ж��Ƿ���Զ�̶��㣬�����Զ�̶��㣬���ڻ�����и�����״̬
		for(int i=0; i<frontier_vertexes.size(); i++)
		{
			VertexT * v = frontier_vertexes[i];
            // ��Ϊ�Ǹ��»�����ж����״̬������������ǻ����Զ�̶��㣬�����Ҫ���ж϶����Ƿ�ΪԶ�̶���
            // �� hash(v->id) != _my_rank ͨ���ж��Ƿ�ΪԶ�̶���
			if(hash(v->id) != _my_rank) vcache.unlock(v->id); // �����Զ�̶��㣬����Ҫ�ڻ�����и�����Ӧ��״̬��������ʹ�øö�������������� 1��������� 0���Ǹö����� ��-tables �Ƶ� Z-tables ��
		}
	}

    /**
     * Զ�̶������ݷ��غ󣬽�Զ�̶������õ���ȡ�����б�frontier_vertexes����
     * �� Comper.push_task_from_taskmap() �е���
     */
	//task_map => task_buf => push_task_from_taskmap() (where it is called)
	void set_pulled() //called after vertex-pull, to replace NULL's in "frontier_vertexes"
	{
        // ������ȡ�����б�frontier_vertexes���������Զ�̶��㣬��ӻ������ȡ����Զ�̶��㣬Ȼ��Ž���ȡ�����б�frontier_vertexes����
		CTable & vcache = *(CTable *)global_vcache; 
		for(int i=0; i<to_pull.size(); i++)
		{
			if(frontier_vertexes[i] == NULL)
			{
				frontier_vertexes[i] = vcache.get(to_pull[i]);
			}
		}
	}

	friend ibinstream& operator<<(ibinstream& m, const Task& v)
	{
		m << v.subG;
		m << v.context;
		m << v.to_pull;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Task& v)
	{
		m >> v.subG;
		m >> v.context;
		m >> v.to_pull;
		return m;
	}

	friend ifbinstream& operator<<(ifbinstream& m, const Task& v)
	{
		m << v.subG;
		m << v.context;
		m << v.to_pull;
		return m;
	}

	friend ofbinstream& operator>>(ofbinstream& m, Task& v)
	{
		m >> v.subG;
		m >> v.context;
		m >> v.to_pull;
		return m;
	}
};

#endif /* TASK_H_ */
