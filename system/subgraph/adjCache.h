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
 * ������װ�࣬��������Զ�̶��㣬��Ҫ��Ӧ���������µ�������
 * ��-tables������ȡ����Զ�̶��㻺�����¼����ȡ�����ص�Զ�̶��㣬��������Ķ����Ѿ���Զ�� worker ����ȡ�����أ�ʵ����Ҳ���߳�����ʹ�õ�Զ�̶��� map��
 * R-tables�����������Զ�̶��㻺�����¼������Щ���������Զ�̶��㣬������Щ����������Զ�� worker ���󣬵��ǻ�û�з��ء�
 * Z-tables��������ʹ�õ�Զ�̶��㻺�����¼Ŀǰû������ʹ�õ�Զ�̶���
 */

#ifndef ADJCACHE_H_
#define ADJCACHE_H_

/*
The cache of data objects wraps cuckoo hashmap: http://efficient.github.io/libcuckoo
Maintaining a lock_counter with each table entry
Two sub-structures:
1. zeroCache: keeps only those entrys with lock_counter == 0, for ease of eviction���������е� Z-table
2. pullCache: keeps objects to pull from remote���������е� R-tables
*/

#include <cassert>
#include <queue>
#include <atomic>
#include "ReqQueue.h" //for inserting reqs, triggered by lock_and_get(.)
#include "TaskMap.h" //an input to lock_and_get(.)
#include "util/conmap.h"
#include "util/conmap0.h"

using namespace std;

//====== global counter ======
atomic<int> global_cache_size(0); //cache size: "already-in" + "to-pull" ��������ѻ������������������ ��-table �� R-table �������������
int COMMIT_FREQ = 10; //delta that needs to be committed from each local counter to "global_cache_size"
//parameter to fine-tune !!!

//====== thread counter ======
struct thread_counter
{
    int count;
    
    thread_counter()
    {
        count = 0;
    }
    
    void increment() //by 1, called when a vertex is requested over pull-cache for the 1st time �������ǵ�һ������ʱ����ôֵΪ 1
    {
        count++;
        if(count >= COMMIT_FREQ)
        {
            global_cache_size += count;
            count = 0; //clear local counter
        }
    }
    
    void decrement() //by 1, called when a vertex is erased by vcache
    {
        count--;
        if(count <= - COMMIT_FREQ)
        {
            global_cache_size += count;
            count = 0; //clear local counter
        }
    }
};

//====== pull-cache (to keep objects to pull from remote) ======
//tracking tasks that request for each vertex
/**
 * ��¼��ҪԶ����ȡ������������� KeyType һ�����ʹ�ã��Ž� map �У������� conmap0<KeyType, TaskIDVec>�����Ӷ����Լ�¼���� key ����������б�
 */
struct TaskIDVec
{
	vector<long long> list; //for ease of erase ��¼����� id�����ں����������Ϊ TaskIDVec ���� KeyType һ�����ʹ�ã��Ž� map �У�������ʵ���Ͼ��Ǽ�¼���󶥵� key ������

	TaskIDVec(){}

	TaskIDVec(long long first_tid)
	{
		list.push_back(first_tid);
	}
};

/**
 * ������ȡ����Ļ�����������е� R-table�������ڱ��涥�����ȡ����
 */
//pull cache
template <class KeyType>
class PullCache
{
public:
    typedef conmap0<KeyType, TaskIDVec> PCache; //value is just lock-counter map
    //we do not need a state to indicate whether the vertex request is sent or not ����Ҫʹ��һ��״̬λ����Ƕ��������Ƿ��Ѿ�����
    //a request will be added to the sending stream (ReqQueue) if an entry is newly inserted ���������һ����Ԫ�أ���һ������ ReqQueue �����һ������
    //otherwise, it may be on-the-fly, or waiting to be sent, but we only merge the reqs to avoid repeated sending ��������Ԫ�ز�����Ԫ�أ����Ѿ��� PCache �У�����ô�Ͳ����ظ��� ReqQueue ����������󣨼��ϲ����󣬱����ظ����ͣ�
    PCache pcache; // �������󻺴��б��������е� R-table���������ǰ��ն��� key �����ֵģ���������ҹ�����key ��ͬ������ͬ��key ��ͬ��Ϊͬһ������
    
    /**
     * ������ key �Ӷ������󻺴��б���ɾ��������������� key �����������������ֵʵ���Ͼ��� tid_collector.size�����Լ���������ȡ�� key ����������б������� tid_collector��
     *
     * @param key               ��ɾ���Ķ���� key
     * @param tid_collector     ������Ͳ��������ʾ������ key ����������б����洢������ key ���������
     */
    //subfunctions to be called by adjCache
    size_t erase(KeyType key, vector<long long> & tid_collector) //returns lock-counter, to be inserted along with received adj-list into vcache
    {
    	conmap0_bucket<KeyType, TaskIDVec> & bucket = pcache.get_bucket(key);
    	hash_map<KeyType, TaskIDVec> & kvmap = bucket.get_map();
    	auto it = kvmap.find(key);
    	assert(it != kvmap.end()); //#DEBUG# to make sure key is found ȷ�� key ������ pcache �д���
    	TaskIDVec & ids = it->second; // ���� key ����������б����С�������� R-table �е� lock-count�������� key �������������
    	size_t ret = ids.list.size(); //record the lock-counter before deleting the element 
		assert(ret > 0); //#DEBUG# we do not allow counter to be 0 here ��Ϊ key ������ pcache �У������󶥵��������һ������� 0
		tid_collector.swap(ids.list); // tid_collector �� ids.list �������Ӷ����Խ� ids.list ��ֵͨ�� tid_collector ���س�ȥ
		kvmap.erase(it); //to erase the element ������ key �Ӷ������󻺴��б���ɾ��
        return ret;
    }
    
    /**
     * �ж�һ����ȡ key ����������Ƿ�Ϊ�����󣨼����ݶ���� key ���ж��Ƿ������󣩡� 
     * �����һ�������󣨼��ڶ�����ȡ���󻺴��б���δ�ҵ���ȡ key ���������Ҳ����˵֮ǰû����������� key ���㣩���򷵻� true��
     * ���򣬷��� false�����������Ѿ��ڶ�����ȡ���󻺴��б� pcache �С�Ҳ����˵֮ǰ�Ѿ���������Ҫ���� key ���㣬����Ѿ��� key �������ȡ����Ž������󻺴��б��У�
     *
     * ͬʱ��¼��Ҫ���� key ���������Ŀ����Ϊ�˺ϲ������ͬ�����󣬼��������ͬʱ��ȡ key ���㣬��ô������������ʵ���Ͽ��Ժϲ���һ������
     * ����֮�����������ͬʱ���� key ���㣬û�б�Ҫÿ����������һ�� key ���㣬��ǰ worker ����ֻ����һ�� key ���㣬�Ȼ�ȡ�� key �������Щ�������һ��ʹ�á�
     *
     * @param key       ����ȡ�Ķ���� key�����ݸ�ֵ�����Ƿ�Ϊһ��������
     * @param counter   
     * @param task_id   ���� id�����˴���ȡ key ���������� id
     */
    bool request(KeyType key, thread_counter & counter, long long task_id) //returns "whether a req is newly inserted" (i.e., not just add lock-counter)
    {
        // ���ݶ��� key �ڶ�����ȡ���󻺴��б�pcache���л�ȡ��Ӧ������
    	conmap0_bucket<KeyType, TaskIDVec> & bucket = pcache.get_bucket(key); // ��ȡ���� key ��Ӧ�� bucket
		hash_map<KeyType, TaskIDVec> & kvmap = bucket.get_map(); // ��ȡ bucket �ڲ��洢����� map
		auto it = kvmap.find(key); // ��ȡ key ��Ӧ������
    	if(it != kvmap.end())
    	{
            // ��ȡ���� key ��Ӧ������
    		TaskIDVec & ids = it->second; // ȡ�� map �� Key ��Ӧ��ֵ 
    		ids.list.push_back(task_id); // ��Ϊ task_id ������Ҫ��ȡ key ���㣬�����Ҫ�� task_id ���뵽 key ����������б���
    		return false;
    	}
    	else
    	{
            // �� pcache ��û�л�ȡ������ key ��Ӧ��������ֱ���� pcache �з��������
            counter.increment();
    		kvmap[key].list.push_back(task_id); // ��Ϊ kvmap �в����� key �����ʹ�� kvmap[key] ʱ���� map �в���һ�� key ��Ԫ�أ�value ȡĬ��ֵ�������� value Ĭ��ֵ��Ϊ TaskIDVec �ǣ���Ȼ�󷵻� value �������᷵�� NULL����
            // �����֮���������д����е� kvmap[key] ���� map �в���һ�� key ��Ԫ�أ������䷵��
            // ���д�������õ�ͬ�ڣ��� new һ�� TaskIDVec��Ȼ�� task_id ���� TaskIDVec �У�����ٽ� TaskIDVec ���� map ��
    		return true;
    	}
    }
};


/**
 * ���󻺴棬�� RespServer��Task��Worker �о���ʹ��
 */
//====== object cache ======

template <class TaskT>
class AdjCache
{
public:

	typedef typename TaskT::VertexType ValType; // ������������ Vertex<KeyT, ValueT, HashT> 
	typedef typename ValType::KeyType KeyType; // ����� Key ��������

	typedef TaskMap<TaskT> TaskMapT; // �������� map ��������

	ReqQueue<ValType> q_req; // �������
    
    /**
     * conmap �� map ��ŵ�Ԫ�����ͣ���ʵ���Ͼ��������� ��-tables �洢��Ԫ������
     */
    //an object wrapper, expanded with lock-counter
    struct AdjValue
    {
        ValType * value;//note that it is a pointer !!! ��������
        int counter; //lock-counter, bounded by the number of active tasks in a machine ��¼ value ������㱻���ٸ�����ʹ�ã���ʹ�� value ������������������ܻ����л�Ծ��������������
    };
    
    //internal conmap for cached objects
    typedef conmap<KeyType, AdjValue> ValCache; // ���� map ���ͣ���һ��������ϣ map��������� ��-tables��Z-table
    ValCache vcache; // ���滺����󣬷�װ�������е� ��-tables������ȡ����Զ�̶��㻺������߳�����ʹ�õĶ��� map���� Z-table����¼Ŀǰû������ʹ�õĶ���� id��
    PullCache<KeyType> pcache; // ������ȡ���󻺴����װ�������е� R-table
    
    ~AdjCache()
    {
    	for(int i=0; i<CONMAP_BUCKET_NUM; i++)
    	{
    		conmap_bucket<KeyType, AdjValue> & bucket = vcache.pos(i);
    		bucket.lock();
    		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
    		for(auto it = kvmap.begin(); it != kvmap.end(); it++)
    		{
    			delete it->second.value; //release cached vertices
    		}
    		bucket.unlock();
    	}
    }
    
    /**
     * �� comper���������̣߳����ã����ڻ�ȡ key ��Ӧ��Զ�̶��㡣�ڴ˺����У������ add2map �����Ὣ����Ž��������� map �С��� Task.pull_all �б����á�
     * ����ڱ��ػ�����û���ҵ� key ��Ӧ�Ķ��㣬���� NULL�����һ���Զ�� worker �������󣬻�ȡ���㡣
     * ����ҵ����򷵻ظö��㡣
     *
     * ע�⣺�˺����л���� add2map
     *
     * �˺����� Task.pull_all �б����ã����ó�����
     * task_id ����������Զ�̶���ʱ�����ػ�����û����Ӧ��Զ�̶��㣬��Ҫ��Զ�� worker �������󣬴Ӷ���ȡ�������ݡ�
     * ��� task_id �����ǵ�һ����Զ�� worker ���Ͷ������󣬸�������Ҫ����״̬������Ҫ���ñ������������������
     *
     * @param key       ����ȡ�Ķ��� key
     * @param counter   
     * @param task_id   ����� id
     * @param taskmap   �����Ӧ�� map�������˸�����ľ�������͹������� 
     * @param task      ����
     */
    //to be called by computing threads, returns:
    //1. NULL, if not found; but req will be posed
    //2. vcache[key].value, if found; vcache[counter] is incremented by 1
    ValType * lock_and_get(KeyType & key, thread_counter & counter, long long task_id,
    		TaskMapT & taskmap, TaskT* task) //used when vcache miss happens, for adding the task to task_map ����� add2map �����Ὣ����Ž��������� map ��
    {
    	ValType * ret; // ����ֵ
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key); // ��ȡ�������ڵ� bucket 
    	bucket.lock();
    	hash_map<KeyType, AdjValue> & kvmap = bucket.get_map(); // ��ȡ bucket �е� map����������ͨ������� key ȡ������
    	auto it = kvmap.find(key); // ȡ����������
    	if(it == kvmap.end())
		{   // Զ�̶���
            // �� vcache �в���ȡ�� key ��Ӧ�Ķ������ݣ������ add2map������������������ map ��
			taskmap.add2map(task); // ��Ϊ��������Ҫͨ��������������ȡԶ�̶��㣬�����Ҫ��������Ž��������� map ��

            // ��Ϊ��Զ�̶��㣬������Ҫ�������󣬼�Ҫ��������������һ��������
            // ���������֮ǰ�����жϸ������Ƿ���һ�������󣨼��ж�֮ǰ�Ƿ���������������� key��
            // �������������֮ǰû������������ö��㣬��ô���������Ҳ������ڸ����������Ҫ���뵽�����С�
            // ���򣬸ö�����֮ǰ�Ѿ���������������������Ѿ�������������У�����Ҫ�ٲ��롣
			bool new_req = pcache.request(key, counter, task_id); // �ж��ڶ�����ȡ���󻺴�����Ƿ����� key �����Ӧ������
			if(new_req) q_req.add(key); // �����һ���������򽫸�������뵽���������
			ret = NULL;
		}
    	else
    	{   // �ڱ��ػ������ҵ�Զ�̶���
            // �� vcache ����ȡ�� key ��Ӧ�Ķ�������
        	AdjValue & vpair = it->second;
        	if(vpair.counter == 0) bucket.zeros.erase(key); //zero-cache.remove ��ǰ����֮ǰ�� zeros �У���֮ǰû�б�����ʹ�ù���������Ҫ����� zeros ��ɾ������Ϊ�ö�����ڱ���������ʹ�ã�
        	vpair.counter++; // �ö�����ڵ�ǰ������ʹ�ã����Ըö����Ӧ�� counter �� 1
        	ret = vpair.value; // ���ظö���
    	}
    	bucket.unlock();
    	return ret;
    }
    
    /**
     * �� comper���������̣߳����ã����ڻ�ȡ key ��Ӧ��Զ�̶��㡣�ڴ˺����У�������� add2map �������Ὣ����Ž��������� map �С��� Task.pull_all �б����á�
     * ���û���ҵ� key ��Ӧ�Ķ��㣬���� NULL�����һ���Զ�� worker �������󣬻�ȡ���㡣
     * ����ҵ����򷵻ظö��㡣
     * 
     * ע�⣺�˺����в����� add2map
     * 
     * �˺����� Task.pull_all �б����ã����ó�����
     * task_id ������֮ǰ����Զ�̶���ʱ���Ѿ����Ž��������� map �У�����������������Զ�̶���ʱ�������ٽ������������� map ��
     */
    //to be called by computing threads, returns:
	//1. NULL, if not found; but req will be posed
	//2. vcache[key].value, if found; vcache[counter] is incremented by 1
	ValType * lock_and_get(KeyType & key, thread_counter & counter, long long task_id) // ������ add2map �������Ὣ����Ž��������� map ��
	{
		ValType * ret;
		conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		if(it == kvmap.end())
		{
			bool new_req = pcache.request(key, counter, task_id);
			if(new_req) q_req.add(key);
			ret = NULL;
		}
		else
		{
			AdjValue & vpair = it->second;
			if(vpair.counter == 0) bucket.zeros.erase(key); //zero-cache.remove
			vpair.counter++;
			ret = vpair.value;
		}
		bucket.unlock();
		return ret;
	}

    /**
     * �ӱ��ػ�����ȡ�� key ��Ӧ�Ķ��㡣����ǰ��Ҫȷ�� key ������ map �д��ڡ�
     *
     * @param key   ����ȡ�Ķ���� id 
     */
	//must lock, since the hash_map may be updated by other key-insertion
	ValType * get(KeyType & key) //called only if you are sure of cache hit
	{
		conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		assert(it != kvmap.end());
		ValType * val = it->second.value;
		bucket.unlock();
		return val;
	}

    /**
     * ���� key ���㣬�������߳�ʹ������ key �������øú�����
     * ͬʱ��ʹ�øö������������ 1�����ʹ�øö����������Ϊ 0����û�������ʹ�øö��㣩���򽫶����Ƶ� zero �����С�
     */
    //to be called by computing threads, after finishing using an object
    //will not check existence, assume previously locked and so must be in the hashmap
    void unlock(KeyType & key)
    {
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
    	bucket.lock();
    	hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		assert(it != kvmap.end());
    	it->second.counter--; // ��Ϊ�ü����߳��Ѿ�ʹ���� key ���㣬���ʹ�� key �������������Ҫ�� 1
    	if(it->second.counter == 0) bucket.zeros.insert(key); //zero-cache.insert ��Ϊ�Ѿ�û������ʹ�� key ���㣬��˽��ö������ zeros ������
    	bucket.unlock();
    }

    /**
     * �� id Ϊ key��ֵΪ value �Ķ�������󻺴��б���ɾ����Ȼ����뵽����ȡ����Զ�̶��㻺��� �У��� ��-tables����
     * �ú�����ͨ���߳� RespServer ���ã�RespServer.thread_func() ������������ͨ���߳̽��յ����ص�Զ�̶������ݺ󣬵��øú�����
     * 
     * �ú����Ĺ���ʵ����Ҳ���ǽ� key ��������󻺴��б� pcache��R-table�����Ƶ�����ȡ����Զ�̶��㻺��� �У��������е� vcache�������е� ��-table��
     * 
     * @param key               ����������ͣ����� id
     * @param value             ����������ͣ�����ֵ     
     * @param tid_collector     ����������ͣ���������� key ��������� id
     */
    //to be called by communication threads: pass in "ValType *", "new"-ed outside
    //** obtain lock-counter from pcache
    //** these locks are transferred from pcache to the vcache
    void insert(KeyType key, ValType * value, vector<long long> & tid_collector) //tid_collector gets the task-id list
	{
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		AdjValue vpair;
		vpair.value = value;
		vpair.counter = pcache.erase(key, tid_collector); // �� key ��������󻺴��б���ɾ������Ϊ�ö����Ѿ���ȡ���ˣ���ͬʱ��������� key ��������񣨱����� tid_collector����������
		bool inserted = bucket.insert(key, vpair); // ���������ݲ��뵽����ȡ����Զ�̶��㻺��� �У��� ��-tables��
		assert(inserted);//#DEBUG# to make sure item is not already in vcache ����Ҫȷ������ɹ�����ȷ������ȡ����Զ�̶��㻺��� �в����ڶ��� key��
        // ��Ϊ���� key ��ͨ��Զ�������ȡ���ģ���˵����������ʹ�õĶ��� map ��һ�������ڸö��㣨������ڵĻ�����ôҲ����ͨ��Զ�������ȡ��
		//#DEBUG# this should be the case if logic is correct:
		//#DEBUG# not_in_vcache -> pull -> insert
		bucket.unlock();
	}
    
    //note:
    //1. lock_and_get & insert operates on pull-cache ��lock_and_get �� insert �������� pull-cache �ϲ�����
    //2. pcache.erase/request(key) should have no conflict, since they are called in vcache's lock(key) section
    
    int pos = 0; //starting position of bucket in conmap ��conmap �е�ǰ bucket ���±꣬Ҳ���� Z-tables����-tables �е�ǰ bucket ���±꣬��ɾ�� Z-tables����-tables �Ļ�������ʱʹ�ã���ɾ�� pos �� bucket ��

    //try to delete "num_to_delete" elements, called only if capacity VCACHE_LIMIT is reached
    //insert(.) just moves data from pcache to vcache, does not change total cache capacity
    //calling erase followed by insert is not good, as some elements soon to be locked (by other tasks) may be erased
    //we use the strategy of "batch-insert" + "trim-to-limit" (best-effort)
    //if we fail to trim to capacity limit after checking one round of zero-cache, we just return
    /**
     * ���� Z-tables ɾ�� ��-tables ��������ʹ�õĶ���Ļ������ݣ����� ��-tables �У���Щ����û�б�����ʹ�ã�����Щ����Ļ������ݿ���ɾ�������Ӷ��ͷ��ڴ�ռ䣩��
     * ���ص���ɾ��ʧ�ܵĶ������
     * 
     * @param num_to_delete     ��ͼɾ���Ķ������
     * @param counter           
     */
    size_t shrink(size_t num_to_delete, thread_counter & counter) //return value = how many elements failed to trim
    {
    	int start_pos = pos;
		//------
		while(num_to_delete > 0) //still need to delete element(s)
		{
			conmap_bucket<KeyType, AdjValue> & bucket = vcache.pos(pos);
			bucket.lock();
            // ���� Z-tables������һ�����ϣ���¼��Ŀǰû�� task ʹ�õĶ��㣩��ȡ������ key ��Ȼ���� Z-tables����-tables �н����� key �Ļ�������ɾ��
			auto it = bucket.zeros.begin();
			while(it != bucket.zeros.end())
			{
				KeyType key = *it; // zeros �ж���� key
				hash_map<KeyType, AdjValue> & kvmap = bucket.get_map(); // ����ȡ����Զ�̶��㻺�����Ӧ�����е� ��-tables
				auto it1 = kvmap.find(key);
				assert(it1 != kvmap.end()); //#DEBUG# to make sure key is found, this is where libcuckoo's bug prompts
				AdjValue & vpair = it1->second;
				if(vpair.counter == 0) //to make sure item is not locked ȷ�� key ����ȷʵû������ʹ�ã��Ӷ����ö���ӻ������ɾ��
				{
                    // ɾ�� Z-tables����-tables �л��������
					counter.decrement();
					delete vpair.value;
					kvmap.erase(it1);
					it = bucket.zeros.erase(it); //update it ���� it λ����һ��Ԫ�صĵ������������� it
					num_to_delete--;
					if(num_to_delete == 0) //there's no need to look at more candidates
					{
						bucket.unlock();
						pos++; //current bucket has been checked, next time, start from next bucket ��һ�ε��ñ�����ʱ���� Z-tables ����һ�� bucket ��ʼɾ��
						return 0; // ��Ϊ�Ѿ�����Ҫɾ���Ķ���ȫ��ɾ������ɾ��ʧ�ܵ�
					}
				}
			}
			bucket.unlock();
			//------
			//move set index forward in a circular manner
			pos++;
			if(pos >= CONMAP_BUCKET_NUM) pos -= CONMAP_BUCKET_NUM; // �Ѿ����������һ�� bucket ����ʱ pos = CONMAP_BUCKET_NUM����������� CONMAP_BUCKET_NUM������ pos ����Ϊ 0
			if(pos == start_pos) break; //finish one round of checking all zero-sets, no more checking ���Ѿ��� Z-tables����-tables �����е� bucket �����ر���һ�飬���ٽ�����һ�ֱ���������ɾ��������
            // �ж��Ƿ������һ�� bucket �ı�־�� ��pos == start_pos���������� ��pos == start_pos�� ��
            // ������Ϊ���������ܻ������� return ��������������������£��´ν��뱾����ʱ��pos ��Ϊ 0������ʼλ�ò�Ϊ 0��������Ӧ���� start_pos ���бȽϣ��������� 0 ���бȽ�
            // �� Z-tables����-tables �����е� bucket ֻ����һ�飬����Ϊ������һ���Z-tables ���Ѿ�ɾ���ˣ���˲���Ҫ�ٽ���ɾ����
            // �� num_to_delete ��ֵ�ϴ󣬴��� Z-tables ��Ԫ�ظ���ʱ����ô���շ��ص� num_to_delete �Ͳ���Ϊ 0������ num_to_delete �� ��ɾ��ʧ�ܡ� �Ķ��㡣
		}
		return num_to_delete; // num_to_delete ������ɾ��ʧ�ܵĶ������
    }

};

#endif
