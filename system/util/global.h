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

#ifndef GLOBAL_H
#define GLOBAL_H

#include <mpi.h>
#include <stddef.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include <assert.h> //for ease of debug
#include <sys/stat.h>
#include <ext/hash_set>
#include <ext/hash_map>

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include <atomic>
#include <mutex>
#include "conque.h"

#include "rwlock.h"

//============================
#include <time.h>

int POLLING_TIME; //unit: usec, user-configurable, used by sender
//set in init_worker()
static clock_t polling_ticks; // = POLLING_TIME * CLOCKS_PER_SEC / 1000000;

#define SLEEP_PARAM 2000 // POLLING_TIME = SLEEP_PARAM * _num_workers
//this ratio is tested on Azure
//if too small, communication is unbalanced and congestion happens
//if too big, low bandwidth utilization

#define MAX_BATCH_SIZE 1000 //number of bytes sent in a batch

#define WAIT_TIME_WHEN_IDLE 100 //unit: usec, user-configurable, used by recv-er

#define STATUS_SYNC_TIME_GAP 100000 //unit: usec, used by Worker main-thread

#define AGG_SYNC_TIME_GAP 1000000 //unit: usec, used by AggSync main-thread

#define PROGRESS_SYNC_TIME_GAP 1000000 //unit: usec, used by Profiler main-thread

#define TASK_BATCH_NUM 150 //minimal number of tasks processed as a unit
#define TASKMAP_LIMIT 8 * TASK_BATCH_NUM //number of tasks allowed in a task map

#define VCACHE_LIMIT 2000000 //how many vertices allowed in vcache (pull-cache + adj-cache)
#define VCACHE_OVERSIZE_FACTOR 0.2
#define VCACHE_OVERSIZE_LIMIT VCACHE_LIMIT * VCACHE_OVERSIZE_FACTOR

#define MAX_STEAL_TASK_NUM 10*TASK_BATCH_NUM //how many tasks to steal at a time at most 一次最多可以窃取的任务数量
#define MIN_TASK_NUM_BEFORE_STEALING 10*TASK_BATCH_NUM //how many tasks should be remaining (or task stealing is triggered) 各个 worker 中应该保留的最小任务数量，如果低于这个值，则该 worker 的负载较轻，则需要从负载较重的 worker 中窃取任务执行

#define MINI_BATCH_NUM 10 //used by spawning from local
#define REQUEST_BOUND 50000 //the maximal number of requests could be sent between each two workers //tuned on GigE

#define GRAPH_LOAD_CHANNEL 200
#define REQ_CHANNEL 201
#define RESP_CHANNEL 202
#define STATUS_CHANNEL 203
#define AGG_CHANNEL 204
#define PROGRESS_CHANNEL 205

void* global_trimmer = NULL;
void* global_taskmap_vec; //set by Worker using its compers, used by RespServer
void* global_vcache;
void* global_local_table;

atomic<int> global_num_idle(0); // 当前 worker 中空闲 comper 的数量

conque<string> global_file_list; //tasks buffered on local disk; each element is a file name 磁盘中保存的 task 文件名
atomic<int> global_file_num; //number of files in global_file_list 磁盘中保存的 task 文件数量

void* global_vertexes;
int global_vertex_pos; //next vertex position in global_vertexes to spawn a task
mutex global_vertex_pos_lock; //lock for global_vertex_pos

#define TASK_GET_NUM 1
#define TASK_RECV_NUM 1
//try "TASK_GET_NUM" times of fetching and processing tasks
//try "TASK_RECV_NUM" times of inserting processed tasks to task-queue
//============================

#define hash_map __gnu_cxx::hash_map
#define hash_set __gnu_cxx::hash_set

using namespace std;

atomic<bool> global_end_label(false); // 标记所有 worker 是否都已经处理完任务，如果已经处理完所有任务，则为 true，结束迭代计算

//============================
///worker info
#define MASTER_RANK 0

int _my_rank;
int _num_workers;
inline int get_worker_id()
{
    return _my_rank;
}
inline int get_num_workers()
{
    return _num_workers;
}

void init_worker(int * argc, char*** argv)
{
	int provided;
	MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided); // 在一个进程中开启多个线程，进程中开启的所有线程都是可以进行MPI Call
	if(provided != MPI_THREAD_MULTIPLE)
	{
	    printf("MPI do not Support Multiple thread\n");
	    exit(0);
	}
	MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
	MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
    POLLING_TIME = SLEEP_PARAM * _num_workers;
    polling_ticks = POLLING_TIME * CLOCKS_PER_SEC / 1000000;
}

void worker_finalize()
{
    MPI_Finalize();
}

void worker_barrier()
{
    MPI_Barrier(MPI_COMM_WORLD); //only usable before creating threads
}

//------------------------
// worker parameters

struct WorkerParams {
    string input_path;
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    WorkerParams()
    {
    	force_write = true;
        native_dispatcher = false;
    }
};

//============================
//general types
typedef int VertexID;

void* global_aggregator = NULL; // worker 的聚合器

void* global_agg = NULL; //for aggregator, FinalT of previous round
rwlock agg_rwlock;

//============================
string TASK_DISK_BUFFER_DIR;
string REPORT_DIR;

//disk operations
void _mkdir(const char *dir) {//taken from: http://nion.modprobe.de/blog/archives/357-Recursive-directory-creation.html
	char tmp[256];
	char *p = NULL;
	size_t len;

	snprintf(tmp, sizeof(tmp), "%s", dir);
	len = strlen(tmp);
	if(tmp[len - 1] == '/') tmp[len - 1] = '\0';
	for(p = tmp + 1; *p; p++)
		if(*p == '/') {
				*p = 0;
				mkdir(tmp, S_IRWXU);
				*p = '/';
		}
	mkdir(tmp, S_IRWXU);
}

void _rmdir(string path){
    DIR* dir = opendir(path.c_str());
    struct dirent * file;
    while ((file = readdir(dir)) != NULL) {
        if(strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
        	continue;
        string filename = path + "/" + file->d_name;
        remove(filename.c_str());
    }
    if (rmdir(path.c_str()) == -1) {
    	perror ("The following error occurred");
        exit(-1);
    }
}

atomic<bool>* idle_set; //to indicate whether a comper has notified worker of its idleness 当前 worker 中所有 comper 的工作状态，在 comper 中设置为 true，在 worker 中设置为 false
mutex mtx_go; // 全局互斥锁，在 Worker 和 Comper 中使用
condition_variable cv_go; // 全局条件变量，在 Worker 和 Comper 中使用

//used by profiler
atomic<size_t>* global_tasknum_vec; //set by Worker using its compers, updated by comper, read by profiler
atomic<size_t> num_stolen(0); //number of tasks stolen by the current worker since previous profiling barrier 当前 worker 窃取到任务总数

atomic<size_t>* req_counter; //to count how many requests were sent to each worker 存储向其它 worker 发送的请求次数

int num_compers; // 一个 worker 所拥有的 comper 数量

//============= to allow long long to be ID =============
namespace __gnu_cxx {
    template <>
    struct hash<long long> {
        size_t operator()(long long key) const
        {
            return (size_t)key;
        }
    };
}
//====================================================

#endif
