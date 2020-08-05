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
 * ���� map��������ϣ��
 * �� 1 ����ϣ��ͨ�� key ��� conmap ����Ӧ�� bucket
 * �� 2 ����ϣ��ͨ�� key ��� bucket ����Ӧ��Ԫ��
 * 
 * �� Map �з�װ�������е� ��-tables���߳�����ʹ�õĶ��� map���� Z-table����¼Ŀǰû������ʹ�õĶ���� id��
 */

#ifndef CONMAP_H
#define CONMAP_H

#define CONMAP_BUCKET_NUM 10000 //should be proportional to the number of threads on a machine ��ϣ���� bucket �ĸ���

//idea: 2-level hashing
//1. id % CONMAP_BUCKET_NUM -> bucket_index
//2. bucket[bucket_index] -> give id, get content

//now, we can dump zero-cache, since GC can directly scan buckets one by one

#include <util/global.h>
#include <vector>
#include <unordered_set>
using namespace std;

/**
 * conmap �е�һ�� bucket �����溬��һ����ϣ�� bucket����Ӧ�����е� ��-tables����һ������ zeros����Ӧ�����е� Z-tables��
 */
template <typename K, typename V> struct conmap_bucket
{
	typedef hash_map<K, V> KVMap;
	typedef unordered_set<K> KSet;
	mutex mtx;
	KVMap bucket; // �洢�߳�����ʹ�õĶ��㣬��Ӧ�����е� ��-tables
	KSet zeros; // ��¼Ŀǰû�� task ʹ�õĶ���� id���� zeros ����Ķ��㶼û�б���������ʹ�ã����԰�ȫ�ش��ڴ���ɾ��������Ӧ�����е� Z-tables

	inline void lock()
	{
		mtx.lock();
	}

	inline void unlock()
	{
		mtx.unlock();
	}

    /**
     * ��ȡ�߳�����ʹ�õĶ��� map
     */
	KVMap & get_map()
	{
		return bucket;
	}

    /**
     * �� id Ϊ key��ֵ Ϊ val �Ķ�����뵽�߳�����ʹ�õĶ��� map �С�
     * �������ɹ������� true�����򣬷��� false���� key �����Ѿ����� map �У�
     *
     * @param key   ������Ķ���� id ֵ 
     * @param val   ������Ķ����ֵ
     */
	//returns true if inserted
	//false if an entry with this key alreqdy exists
	bool insert(K key, V & val)
	{
		auto ret = bucket.insert(
			std::pair<K, V>(key, val)
		);
		return ret.second;
	}

    /**
     * �� key ������߳�����ʹ�õĶ��� map ��ɾ�������ɾ���ɹ������� true�����򣬷��� false��
     * 
     * @param key   ��ɾ���Ķ���� id 
     */
	//returns whether deletion is successful
	bool erase(K key)
	{
		size_t num_erased = bucket.erase(key);
		return (num_erased == 1);
	}
};

/**
 * ���� map�����ж�� bucket
 */
template <typename K, typename V> struct conmap
{
public:
	typedef conmap_bucket<K, V> bucket;
	bucket* buckets;

	conmap()
	{
		buckets = new bucket[CONMAP_BUCKET_NUM];
	}

	bucket & get_bucket(K key)
	{
		return buckets[key % CONMAP_BUCKET_NUM];
	}

    /**
     * �����±�Ϊ pos �� bucket
     */
	bucket & pos(size_t pos)
	{
		return buckets[pos];
	}

	~conmap()
	{
		delete[] buckets;
	}
};

#endif
