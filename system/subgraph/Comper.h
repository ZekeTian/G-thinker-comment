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
 *  线程封装类
 */

#ifndef COMPER_H_
#define COMPER_H_

#include "util/global.h"
#include "util/ioser.h"
#include <deque> //for task queue
#include <unistd.h> //for usleep()
#include "TaskMap.h"
#include "adjCache.h"
#include "string.h"
#include <thread>
#include "Aggregator.h"
#include <fstream>

using namespace std;

template <class TaskT, class AggregatorT = DummyAgg>
class Comper {
public:
    typedef Comper<TaskT, AggregatorT> ComperT;

	typedef TaskT TaskType;
    typedef AggregatorT AggregatorType;

    typedef typename AggregatorT::FinalType FinalT;

	typedef typename TaskT::VertexType VertexT;
	typedef typename TaskT::SubgraphT SubgraphT;
	typedef typename TaskT::ContextType ContextT;

	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    typedef vector<VertexT*> VertexVec;

    typedef deque<TaskT *> TaskQueue;
    typedef TaskMap<TaskT> TaskMapT; // 任务 Map 类型
    typedef hash_map<KeyT, VertexT*> VTable;
    typedef typename VTable::iterator TableIter;

    /**
     * 当前线程的任务队列 
     */
    TaskQueue q_task; //written only by the thread itself, no need to be conque 当前线程的任务队列
    
    /**
     * 当前线程的 id
     */
    int thread_rank; // 线程 id

    /**
     * 存放任务的 map
     */
    TaskMapT map_task;
    
    
    thread_counter counter;

    thread main_thread;

    //UDF1
    virtual void task_spawn(VertexT * v) = 0; //call add_task() inside, will flush tasks to disk if queue overflows
    //UDF2
    virtual bool compute(SubgraphT & g, ContextT & context, vector<VertexT *> & frontier) = 0;

    //task's pull wrappper (to serve UDF2 wrapper)
    TaskT* cur_task;
    void pull(KeyT id){
    	cur_task->to_pull.push_back(id);
	}

    /**
     * 如果任务未结束，则返回 true，表示在下一轮迭代中仍然需要进行计算；否则，返回 false（即任务在此轮迭代中结束）。
     * 该函数会在 comper 的 push_task_from_taskmap 和 pop_task 中调用
     */
    //UDF2 wrapper
    bool compute(TaskT* task)
    {
    	cur_task = task; //so that in UDF2, one can directly call pull(key), no need to get the task object
    	task->to_pull.clear(); //clear it for pulling new vertices
    	return compute(task->subG, task->context, task->frontier_vertexes);
    }

    ofstream fout;

    /**
     * Comper 线程启动
     */
    void start(int thread_id)
    {
    	thread_rank= map_task.thread_rank = thread_id;
    	//------
        // 生成一个文件，文件名格式为：目录/worker的id_线程id
		char file[1000], no[40];
		long long fileSeqNo = 1;
		strcpy(file, REPORT_DIR.c_str());
		sprintf(no, "/%d_%d", _my_rank, thread_rank);
		strcat(file, no);
		fout.open(file);
		//------
    	main_thread = thread(&ComperT::run, this);
    }

    virtual ~Comper()
    {
    	main_thread.join();
    	fout.close();
    }

    /**
     * 从磁盘文件中加载任务，并将任务加入到任务队列中。如果没有磁盘文件，则返回 false；否则，返回 true
     */
    //load tasks from a file (from "global_file_list" to the task queue)
    //returns false if "global_file_list" is empty
    bool file2queue()
    {
    	string file;
    	bool succ = global_file_list.dequeue(file);
    	if(!succ) return false; //"global_file_list" is empty
    	else
    	{
    		global_file_num --;
    		ofbinstream in(file.c_str());
    		while(!in.eof())
    		{
    			TaskT* task;
    			in >> task;
    			add_task(task); // 从文件中反序列化出 task 对象后，将其加入到任务队列中
    		}
    		in.close();

            if (remove(file.c_str()) != 0) {
                cout<<"Error removing file: "<<file<<endl;
                perror("Error printed by perror");
            }
    		return true;
    	}
    }

    /**
     * 根据本地顶点列表中的顶点生成任务。本地顶点列表能再生成新的任务，则返回 true；否则，返回 false
     */
    //load tasks from local-table
	//returns false if local-table is exhausted
    bool locTable2queue()
	{
		size_t begin, end; //[begin, end) are the assigned vertices (their positions in local-table)
		//note that "end" is exclusive
		VTable & ltable = *(VTable *)global_local_table;
		int size = ltable.size(); // 本地顶点列表的大小
		//======== critical section on "global_vertex_pos"
        // 防止多个线程同时对全局本地顶点列表（global_local_table）进行访问，因此先加锁进行同步
		global_vertex_pos_lock.lock();
		if(global_vertex_pos < size) // 还能继续从本地顶点列表中生成任务
		{
			begin = global_vertex_pos; //starting element
			end = begin + TASK_BATCH_NUM; // 一次取出一个批次的任务
			if(end > size) end = size;
			global_vertex_pos = end; //next position to spawn
		}
		else begin = -1; //meaning that local-table is exhausted 本地顶点列表中不能再生成新的任务
		global_vertex_pos_lock.unlock();
		//======== spawn tasks from local-table[begin, end)
        // 从本地顶点列表的 [begin, end) 区间内取出顶点生成任务
		if(begin == -1) return false; // 本地顶点列表中不能再生成新的任务，则返回 false
		else
		{
            VertexVec & gb_vertexes = *(VertexVec*) global_vertexes; // 取出当前 worker 的本地顶点列表
			for(int i=begin; i<end; i++)
			{//call UDF to spawn tasks
				task_spawn(gb_vertexes[i]); // 逐个顶点生成任务
			}
			return true; // 能生成新的任务，则返回 true
		}
	}

    AggregatorT* get_aggregator() //get aggregator
    //cannot use the same name as in global.h (will be understood as the local one, recursive definition)
    {
    	return (AggregatorT*)global_aggregator;
    }


    /**
     * 弹出一个任务并处理它，如果有必要则会加入 task_map 中。
     * 如果队列为空，并且本地顶点列表中不需要处理顶点，则返回 false
     */
    //part 2's logic: get a task, process it, and add to task-map if necessary
    //- returns false if (a) q_task is empty and
    // *** (b) locTable2queue() did not process a vertex (may process vertices but tasks are pruned)
    //condition to call:
    //1. map_task has space
    //2. vcache has space
    bool pop_task()
    {
    	bool task_spawn_called = false;
    	bool push_called = false;
    	//fill the queue when there is space
    	if(q_task.size() <= TASK_BATCH_NUM) // 任务队列中还可以继续存入任务，则向任务队列中继续添加任务
    	{
            // 先从磁盘文件加载任务，如果磁盘文件为空，则从 task-map 中生成任务
    		if(!file2queue()) //priority <1>: fill from file on local disk
    		{//"global_file_list" is empty
    			if(!push_task_from_taskmap()) //priority <2>: fetch a task from task-map
    			{//CASE 1: task-map's "task_buf" is empty // task_buf 为空，则从本地顶点列表中生成新的任务
    				task_spawn_called = locTable2queue(); // 从本地顶点列表中生成任务
    			}
    			else
    			{//CASE 2: try to move TASK_BATCH_NUM tasks from task-map to the queue
                    // task_buf 不为空，则从 task_map 中取出 TASK_BATCH_NUM 个任务，放进队列中
    				push_called = true; // 从 task_buf 中取出任务并压入到队列中
    				while(q_task.size() < 2 * TASK_BATCH_NUM)
    				{//i starts from 1 since push_task_from_taskmap() has been called once 
    					if(!push_task_from_taskmap()) break; //task-map's "task_buf" is empty, no more try
    				}
    			}
    		}
    	}
    	//==================================
    	if(q_task.size() == 0){
			if(task_spawn_called) return true;
			else if(push_called) return true;
			else return false; // 没有顶点需要继续处理，且队列为空，则返回 false
		}
    	//fetch task from Comper's task queue head 从任务队列中取出任务
    	TaskT * task = q_task.front();
    	q_task.pop_front();
    	//task.to_pull should've been set
    	//[*] process "to_pull" to get "frontier_vertexes" and return "how many" vertices to pull
    	//if "how many" = 0, call task.compute(.) to set task.to_pull (and unlock old "to_pull") and go to [*]
    	bool go = true; //whether to continue another round of task.compute() 标记任务是否需要另外一轮计算
    	//init-ed to be true since:  初始为 true 有如下两个原因：
    	//1. if it is newly spawned, should allow it to run
    	//2. if compute(.) returns false, should be filtered already, won't be popped
    	while(task->pull_all(counter, map_task)) //may call add2map(.) task 拉取顶点，如果该任务需要的顶点已经全部拉取到本地，则返回 true，可以继续下一轮迭代；否则，拉取远程顶点，返回 false
    	{
    		go = compute(task);
    		task->unlock_all();
    		if(go == false) // 任务在当前迭代中结束
    		{
    			global_tasknum_vec[thread_rank]++;
				delete task;
    			break; // 任务结束，退出循环
    		}
    	}
    	//now task is waiting for resps (task_map => task_buf => push_task_from_taskmap()), or finished
		return true; // （1）任务在拉取远程顶点，等待响应结果。（2）任务已经结束
    }

    //=== for handling task streaming on disk ===
    char fname[1000], num[20];
    long long fileSeqNo = 1;
    void set_fname() //will proceed file seq #
    {
    	strcpy(fname, TASK_DISK_BUFFER_DIR.c_str());
    	sprintf(num, "/%d_", _my_rank);
    	strcat(fname, num);
    	sprintf(num, "%d_", thread_rank);
    	strcat(fname, num);
    	sprintf(num, "%lld", fileSeqNo);
    	strcat(fname, num);
    	fileSeqNo++;
    }

    /**
     * 添加 task
     */
    //tasks are added to q_task only through this function !!!
    //it flushes tasks as a file to disk when q_task's size goes beyond 3 * TASK_BATCH_NUM
    void add_task(TaskT * task)
    {
    	if(q_task.size() == 3 * TASK_BATCH_NUM)
    	{
            // 当队列中的任务数量等于 3 * TASK_BATCH_NUM 时，则从队列中取出 TASK_BATCH_NUM 个任务保存到文件中
    		set_fname();
    		ifbinstream out(fname);
    		//------
    		while(q_task.size() > 2 * TASK_BATCH_NUM)
    		{
    			//get task at the tail
    			TaskT * t = q_task.back();
    			q_task.pop_back();
    			//stream to file
    			out << t;
    			//release from memory
    			delete t;
    		}
    		out.close();
    		//------
    		//register with "global_file_list"
    		global_file_list.enqueue(fname);
    		global_file_num ++;
    	}
    	//--- deprecated:
    	//task->comper = this;//important !!! set task.comper before entering processing
    	//---------------
    	q_task.push_back(task);
    }

    /**
     * 从 task_map 中取任务，压入到队列中。如果能取出任务，则返回 true；否则，返回 false
     */
    //part 1's logic: fetch a task from task-map's "task_buf", process it, and add to q_task (flush to disk if necessary)
    bool push_task_from_taskmap() //returns whether a task is fetched from taskmap
    {
    	TaskT * task = map_task.get(); // 实际从 task_buf 中取出任务
    	if(task == NULL) return false; //no task to fetch from q_task
    	task->set_pulled(); //reset task's frontier_vertexes (to replace NULL entries)
    	bool go = compute(task); //set new "to_pull" 执行该任务的计算
    	task->unlock_all();
    	if(go != false) add_task(task); //add task to queue 任务还需要继续在下一轮迭代中执行，因此仍然需要将任务加入到队列中
        else
        {
        	global_tasknum_vec[thread_rank]++; // 任务结束，相应 comper 的任务数量加 1
        	delete task;
        }
		return true;
    }

    //=========================

    //combining part 1 and part 2
    void run()
	{
    	while(global_end_label == false) //otherwise, thread terminates
		{
            // 任务没有结束，则继续处理
    		bool nothing_processed_by_pop; //called pop_task(), but cannot get a task to process, and not called a task_spawn(v)
    		bool blocked; //blocked from calling pop_task()
    		bool nothing_to_push; //nothing to fetch from taskmap's buf (but taskmap's map may not be empty)
			for(int i=0; i<TASK_GET_NUM; i++) // TASK_GET_NUM 表示一次取出的任务数量，默认为 1
			{
				nothing_processed_by_pop = false;
				blocked = false;
				//check whether we can continue to pop a task (may add things to vcache)
				if(global_cache_size < VCACHE_LIMIT + VCACHE_OVERSIZE_LIMIT) //(1 + alpha) * vcache_limit
				{
					if(map_task.size < TASKMAP_LIMIT) // 
					{
						if(!pop_task()) nothing_processed_by_pop = true; //only the last iteration is useful, others will be set back to false
					}
					else blocked = true; //only the last iteration is useful, others will be set back to false
				}
				else blocked = true; //only the last iteration is useful, others will be set back to false
			}
			//------
			for(int i=0; i<TASK_RECV_NUM; i++)
			{
				nothing_to_push = false;
				//unconditionally:
				if(!push_task_from_taskmap()) nothing_to_push = true; //only the last iteration is useful, others will be set back to false
			}
			//------
			if(nothing_to_push)
			{
				if(blocked) usleep(WAIT_TIME_WHEN_IDLE); //avoid busy-wait when idle
				else if(nothing_processed_by_pop)
				{
					if(map_task.size == 0) //needed because "push_task_from_taskmap()" does not check whether map_task's map is empty
					{
						unique_lock<mutex> lck(mtx_go);
						idle_set[thread_rank] = true;
						global_num_idle++;
						while(idle_set[thread_rank]){
							cv_go.wait(lck);
						}
					}
					//usleep(WAIT_TIME_WHEN_IDLE); //avoid busy-wait when idle
				}
			}
		}
	}
};

#endif
