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

#include "subg-dev.h"

typedef char Label;

struct AdjItem
{
	VertexID id;
	Label l;
};

struct GMatchValue
{
	Label l;
	vector<AdjItem> adj;
};

typedef Vertex<VertexID, GMatchValue> GMatchVertex;
typedef Subgraph<GMatchVertex> GMatchSubgraph;
typedef Task<GMatchVertex, char> GMatchTask; //context = step

obinstream & operator>>(obinstream & m, AdjItem & v)
{
    m >> v.id;
    m >> v.l;
    return m;
}

ibinstream & operator<<(ibinstream & m, const AdjItem & v)
{
    m << v.id;
    m << v.l;
    return m;
}

ofbinstream & operator>>(ofbinstream & m, AdjItem & v)
{
    m >> v.id;
    m >> v.l;
    return m;
}

ifbinstream & operator<<(ifbinstream & m, const AdjItem & v)
{
    m << v.id;
    m << v.l;
    return m;
}

//------------------
obinstream & operator>>(obinstream & m, GMatchValue & Val)
{
    m >> Val.l;
    m >> Val.adj;
    return m;
}

ibinstream & operator<<(ibinstream & m, const GMatchValue & Val)
{
    m << Val.l;
    m << Val.adj;
    return m;
}

ofbinstream & operator>>(ofbinstream & m, GMatchValue & Val)
{
    m >> Val.l;
    m >> Val.adj;
    return m;
}

ifbinstream & operator<<(ifbinstream & m, const GMatchValue & Val)
{
    m << Val.l;
    m << Val.adj;
    return m;
}

// 输出列表结果
void print_vec(vector<VertexID> vec)
{
	for (int i = 0; i < vec.size(); ++i)
	{
		cout << vec[i] << " ";
	}
	cout << endl;
}

//-------------------
// add a node to graph: only id and label of v, not its edges
// must make sure g.hasVertex(v.id) == true !!!!!!
void addNode(GMatchSubgraph & g, GMatchVertex & v)
{
	GMatchVertex temp_v;
	temp_v.id = v.id;
	temp_v.value.l = v.value.l;
	g.addVertex(temp_v);
}

void addNode(GMatchSubgraph & g, VertexID id, Label l)
{
	GMatchVertex temp_v;
	temp_v.id = id;
	temp_v.value.l = l;
	g.addVertex(temp_v);
}

// add a edge to graph
// must make sure id1 and id2 are added first !!!!!!
void addEdge(GMatchSubgraph & g, VertexID id1, VertexID id2)
{
    GMatchVertex * v1, * v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    AdjItem temp_adj;
	temp_adj.id = v2->id;
	temp_adj.l = v2->value.l;
	v1->value.adj.push_back(temp_adj); // push_back 的时候，会将数据拷贝一份然后保存，因此下面的 temp_adj 在修改值时并不会影响 v1 这里的 Vector
	temp_adj.id = v1->id;
	temp_adj.l = v1->value.l;
	v2->value.adj.push_back(temp_adj);
}

/**
 * 相比 addEdge，本函数会在 add 之前检查插入的边是否已经存在，如果已经存在则不会重复插入边
 */
void addEdge_safe(GMatchSubgraph & g, VertexID id1, VertexID id2) //avoid redundancy
{
    GMatchVertex * v1, * v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    int i = 0;
    vector<AdjItem> & adj = v2->value.adj;
    // 遍历 v2 的邻接表检查是否含有顶点 v1，如果含有的话 i < adj.size()；如果不含有的话，则 i = adj.size()
    for(; i<adj.size(); i++)
    	if(adj[i].id == id1) break;
    if(i == adj.size())
    {
    	AdjItem temp_adj;
		temp_adj.id = v2->id;
		temp_adj.l = v2->value.l;
		v1->value.adj.push_back(temp_adj);
		temp_adj.id = v1->id;
		temp_adj.l = v1->value.l;
		v2->value.adj.push_back(temp_adj);
    }
}

/**
 * 聚合器，用于聚合结果。
 * 聚合器需要指定三个泛型参数，分别为：<ValueT>, <PartialT> and <FinalT>，其含义如下：
 * <ValueT>：本地聚合器（各个 worker 的聚合器）的待聚合数据类型（即 task 中聚合前原始的数据类型）
 * <PartialT>：本地聚合器的数据类型（即各个 task 聚合后的数据类型）
 * <FinalT>：最终的数据类型（即各个 worker 聚合后的数据类型）
 */
class GMatchAgg:public Aggregator<vector<vector<VertexID>>, vector<vector<VertexID>>, vector<vector<VertexID>>>  //all args are counts
{
private:
	vector<vector<VertexID>> part_result; // 各个 worker 的内部聚合的结果（即各个 worker 的匹配结果）
	vector<vector<VertexID>> final_result; // 所有 worker 最终聚合的数据（即最终的匹配结果），只有 master 才会使用（其它 worker 只使用 part_result）

public:

	/**
	 * 初始化聚合器对象
	 */
    virtual void init()
    {
//    	part_result;
//    	cout << "init";
//    	sum = count = 0;
    }

    /**
     * 设置各个 worker 聚合器的初始值
     */
    virtual void init_udf(vector<vector<VertexID>> & prev)
    {
//    	part_result;
//    	cout << "init_udf";
//    	sum = 0;
    }

    /**
     * 各个 worker 聚合各自任务的数据
     *
     * @param task_result 任务执行结束后的最终结果
     */
    virtual void aggregate_udf(vector<vector<VertexID>> & task_result)
    {
    	for (int i = 0; i < task_result.size(); ++i)
    	{
    		part_result.push_back(task_result[i]);
    	}
//    	count += task_count;
    }

    /**
     * master 聚合其它 worker 的结果（此时 master 不会聚合 master 本身的数据）。
     * 具体过程为：将其它 worker 的内部聚合结果聚合到 master 的 final_result 中
     *
     * @param part	其它 worker 聚合的结果
     */
    virtual void stepFinal_udf(vector<vector<VertexID>> & part)
    {
    	for (int i = 0; i < part.size(); ++i)
    	{
    		final_result.push_back(part[i]); // 只聚合其它 worker 的聚合结果，master 自己的聚合结果不在这里聚合
    	}
//    	sum += partial_count; //add all other machines' counts (not master's)
    }

    /**
     * 返回各个 worker 内部聚合后的数据
     *
     * @param collector 输出数据类型，将当前 worker 内部聚合的结果放进 collector 中，从而返回结果
     */
    virtual void finishPartial_udf(vector<vector<VertexID>> & collector)
    {
    	collector = part_result;
//    	collector = count;
    }

    /**
     * master worker 将自己的数据与其它 worker 聚合后的数据（即经过 stepFinal_udf 聚合后的数据，final_result）一起进行最后的一次聚合
     *
     * @param collector 输出参数类型，即将最终所有 worker 聚合的结果放进 collector 中，从而返回结果
     */
    virtual void finishFinal_udf(vector<vector<VertexID>> & collector)
    {
//    	sum += count; //add master itself's count
    	// 将 master 自己内部的聚合结果聚合到最终结果 final_result 中
    	for (int i = 0; i < part_result.size(); ++i)
    	{
    		final_result.push_back(part_result[i]);
    	}

    	if(_my_rank == MASTER_RANK)
    	{
//    		cout<<"the # of matched graph = "<<sum<<endl;
    		cout << "the # of matched graph = " << final_result.size() << endl;
    		cout << "具体的匹配结果如下：" << endl;

    		for (int i = 0; i < final_result.size(); ++i)
    		{
    			cout << "第 " << i << " 个子图匹配结果：";
    			// 输出每一个匹配到的子图结果
    			for (int j = 0; j < final_result[i].size(); ++j) {
    				cout << final_result[i][j] << " ";
    			}
    			cout << endl;
    		}
    	}
//    	collector = sum;
    	collector = final_result;
    }
};

/**
 *	裁剪器，可以对顶点的邻接表进行预处理（一般是用于去除一些无用的顶点）
 */
class GMatchTrimmer:public Trimmer<GMatchVertex>
{
    virtual void trim(GMatchVertex & v) {
    	vector<AdjItem> & val = v.value.adj;
    	vector<AdjItem> newval;
        for (int i = 0; i < val.size(); i++) {
            if (val[i].l == 'a' || val[i].l == 'b' || val[i].l == 'c' || val[i].l == 'd')
            	newval.push_back(val[i]);
        }
        val.swap(newval);
    }
};

vector<vector<VertexID>> graph_matching(GMatchSubgraph & g)
{
	vector<vector<VertexID>> final_match_result; // 最终的匹配结果

	size_t count = 0;
	GMatchVertex & v_a = g.vertexes[0];
	vector<AdjItem> & a_adj = v_a.value.adj;
	vector<VertexID> GMatchQ; //record matched vertex instances
	//------
	GMatchQ.push_back(v_a.id);
	for(int j = 0; j < a_adj.size(); j++) // 顶点 a
	{
		if(a_adj[j].l == 'c') // 顶点 c
		{
			GMatchVertex * v_c = g.getVertex(a_adj[j].id);
			GMatchQ.push_back(v_c->id);
			vector<AdjItem> & c_adj = v_c->value.adj;
			
            // 在 c 的邻接表中寻找顶点 b 
            //find b
			vector<VertexID> b_nodes;
			for(int k = 0; k < c_adj.size(); k++)
			{
				if(c_adj[k].l == 'b')
					b_nodes.push_back(c_adj[k].id);
			}
			if(b_nodes.size() > 1) // 在匹配图中，顶点 c 的邻居顶点集中至少应该有 2 个顶点 b
			{
				vector<bool> b_a(b_nodes.size(), false);//b_a[i] = whether b_nodes[i] links to v_a 标记顶点 b 是否为顶点 a 的邻居点。若 b_a[i] 为 true，则表示 b_nodes[i] 与 a、c 相邻。
				vector<vector<VertexID> > b_d;//b_d[i] = all b-d edges (d's IDs) of b_nodes[i] 匹配图中，b 顶点所有的 b-d 边

                // 遍历顶点 b 集合，将 b 区分成两类，一类是：与顶点 a、c 相邻，另一类是：与顶点 c、d 相邻（类别可以根据 b_a 中的值得到）
				for(int m = 0; m < b_nodes.size(); m++) // 遍历顶点 b
				{
					GMatchVertex * v_b = g.getVertex(b_nodes[m]);
					vector<AdjItem> & b_adj = v_b->value.adj;
					vector<VertexID> vec_d;
					for(int n = 0; n < b_adj.size(); n++)
					{
						if(b_adj[n].id == v_a.id)
							b_a[m] = true; // b_adj[n] 为顶点 a
						if(b_adj[n].l == 'd')
							vec_d.push_back(b_adj[n].id); // // b_adj[n] 为顶点 d
					}
					b_d.push_back(vec_d); // 保存顶点 d 集合
				}
				for(int m = 0; m < b_nodes.size(); m++)
				{
					if(b_a[m]) // 顶点 b 与顶点 a 相邻
					{
						GMatchQ.push_back(b_nodes[m]);//push vertex (3) into GMatchQ 确定 3 号顶点 b（该顶点与 a、c 均相邻）

                        // 遍历 b-d 边集合（实际上也是在遍历顶点 b 集合，b_d 与 b_nodes 长度是一样的），确定 b、d
						for(int n = 0; n < b_d.size(); n++)
						{
							if(m != n) //two b's cannot be the same node 两个顶点 b 不能相同（即顶点 3、4 要是不同的顶点）
							{
								GMatchQ.push_back(b_nodes[n]);//push vertex (4) into GMatchQ
								vector<VertexID> & vec_d = b_d[n]; // 匹配到的顶点 d 集合
//								count += vec_d.size();
								//version that outputs the actual match
								for(int cur = 0; cur < vec_d.size(); cur++)
								{
									GMatchQ.push_back(vec_d[cur]);

									final_match_result.push_back(GMatchQ);
//									cout << "匹配结果" << endl;
//									// 输出最终的匹配结果
//									print_vec(GMatchQ);

									count++;
									GMatchQ.pop_back();//d
								}
								
								GMatchQ.pop_back();//b
							}
						}
						GMatchQ.pop_back();//b
					}
				}
			}
			GMatchQ.pop_back();//c
		}
	}
	GMatchQ.pop_back();//a
	return final_match_result;
}

class GMatchComper:public Comper<GMatchTask, GMatchAgg>
{
public:
    virtual void task_spawn(VertexT * v)
    {
        // 匹配顶点 a 
    	if(v->value.l == 'a')
    	{
    		GMatchTask * t = new GMatchTask;
			addNode(t->subG, *v);
			t->context = 1; //context is step number
			vector<AdjItem> & nbs = v->value.adj;
			bool has_b = false;
			bool has_c = false;
			for(int i=0; i<nbs.size(); i++)
				if(nbs[i].l == 'b')
				{
					t->pull(nbs[i].id);
					has_b = true;
				}
				else if(nbs[i].l == 'c')
				{
					t->pull(nbs[i].id);
					has_c = true;
				}
			if(has_b && has_c) add_task(t);
            else delete t;
    	}
    }

    /**
     * frontier 是 task 中上一步拉取的顶点
     */
    virtual bool compute(SubgraphT & g, ContextT & context, vector<VertexT *> & frontier)
    {
    	//match c 匹配顶点 c
    	if(context == 1) //context is step number
    	{
            // 确定顶点 b(出现在边 a-b, b-c)、顶点 c(出现在边 a-c, c-b)
    		VertexID rootID = g.vertexes[0].id; //root = a-matched vertex 顶点 a
			//cout<<rootID<<": in compute"<<endl;//@@@@@@@@@@@@@
    		// 将当前 task 拉取到的顶点集分成顶点 b 集合、顶点 c 集合
			hash_set<VertexID> label_b; //Vb (set of IDs) 当前 task 顶点 b 的集合（这些顶点 b 与顶点 a 是邻居）
			vector<VertexT *> label_c; //Vc 当前 task 顶点 c 的集合（这些顶点 c 与顶点 a 是邻居）
			for(int i = 0; i < frontier.size(); i++) {//set Vb and Vc from frontier
				if(frontier[i]->value.l == 'b')
					label_b.insert(frontier[i]->id);
				else if(frontier[i]->value.l == 'c')
					label_c.push_back(frontier[i]);
			}

			//------
			hash_set<VertexID> bList; //vertices to pull 待拉取的 b 顶点
			// 遍历顶点 c 集合中的所有顶点的邻接表，确定符合条件的顶点 c
			for(int i = 0 ; i < label_c.size(); i++)
			{
				VertexT * node_c = label_c[i]; //get v_c
				vector<AdjItem> & c_nbs = node_c->value.adj; //get v_c's adj-list
				// U1 中存储的顶点 b ，既是 c 的邻居同时又是 a 的邻居
                // U2 中存储出顶点 b，只是 c 的邻居但不是 a 的邻居
				vector<VertexID> U1, U2;
				// 遍历当前顶点 c 的邻接表
				for(int j = 0; j < c_nbs.size(); j++) //set U1 & U2
				{
					AdjItem & nb_c = c_nbs[j];
					if(nb_c.l == 'b')
					{
						VertexID b_id = nb_c.id;
						if(label_b.find(b_id) != label_b.end())
							U1.push_back(b_id); // 顶点 b 既是 c 的邻居同时又是 a 的邻居
						else
							U2.push_back(b_id); // 顶点 b 只是 c 的邻居但不是 a 的邻居
					}
				}
				//------
				if(U1.empty()) // 不存在满足条件的顶点 b（即同时是顶点 a、c 的邻居点），则当前顶点 c 不满足条件，继续匹配下一个顶点 c
					continue;
				else if(U1.size() == 1)
				{
					if(U2.empty()) continue;
					else
						bList.insert(U2.begin(), U2.end());
				}
				else
				{
					bList.insert(U1.begin(), U1.end()); // 为什么 U1 集合中的顶点 b 也要拉取
					bList.insert(U2.begin(), U2.end());
				}
				//------
				// 当前顶点 c 满足条件，则将当前顶点加入到图中
				//add v_c and edges (v_a, v_c)
				addNode(g, *node_c); // 添加顶点 c
				addEdge(g, rootID, node_c->id); // 添加边 a-c
				//------
				for(int j = 0; j < U1.size(); j++)
				{
					if(!g.hasVertex(U1[j]))
					{
						addNode(g, U1[j], 'b'); //add v_b
						addEdge(g, rootID, U1[j]); //add (v_a, v_b) 添加边 a-b
					}
					addEdge(g, node_c->id, U1[j]); //add (v_c, v_b) //this is forgotten to mention in CoRR's version 添加 c-b
				}
			}
			//pull bList
			for(auto it = bList.begin(); it != bList.end(); it++) pull(*it);
			//cout<<rootID<<": step 1 done"<<endl;//@@@@@@@@@@@@@
			//------
			context++;
			return true;
    	}
    	else //step == 2
    	{
            // 确定顶点 b(出现在边 c-b、b-d)、d(出现在 b-d)
    		hash_set<VertexID> Vc; // 存储当前任务子图中的顶点 c
    		//get Vc from g
    		for(int i=0; i<g.vertexes.size(); i++)
    		{
    			VertexT & v = g.vertexes[i];
    			if(v.value.l == 'c') Vc.insert(v.id);
    		}
    		//------
    		for(int i=0; i<frontier.size(); i++) // 遍历拉取到的顶点 b
    		{
    			VertexT* v_b = frontier[i]; // 存储顶点 b
    			vector<AdjItem> & adj_b = v_b->value.adj; // 顶点 b 的邻接表
    			//construct Vd
    			vector<VertexID> Vd;
    			for(int j=0; j<adj_b.size(); j++)
    			{
    				if(adj_b[j].l == 'd') Vd.push_back(adj_b[j].id); // 顶点 d
    			}
    			//------
    			if(!Vd.empty())
    			{
    				//add v_b
    				if(!g.hasVertex(v_b->id)) addNode(g, *v_b);
					//add (v_b, v_c), where v_c \in bList
					for(int j=0; j<adj_b.size(); j++)
					{
						VertexID b_id = adj_b[j].id; //v_c
						if(Vc.find(b_id) != Vc.end()) // 判断顶点 v_c 是否在当前任务的子图中
						{
							//add edge v_b, v_c
							addEdge_safe(g, b_id, v_b->id); // 添加边 b-c
						}
					}

                    // 加入顶点 d 和边 b-d
    				//add v_d and (v_b, v_d)
    				for(int j=0; j<Vd.size(); j++)
    				{
    					if(!g.hasVertex(Vd[j])) addNode(g, Vd[j], 'd');
    					addEdge(g, Vd[j], v_b->id);
    				}
    			}
    		}
    		//run single-threaded mining code
    		vector<vector<VertexID>> match_result = graph_matching(g);
			GMatchAgg* agg = get_aggregator();
//			agg->aggregate(count);
			agg->aggregate(match_result);
			//cout<<rootID<<": step 2 done"<<endl;//@@@@@@@@@@@@@
			return false;
    	}
    }
};

class GMatchWorker:public Worker<GMatchComper>
{
public:
	GMatchWorker(int num_compers) : Worker<GMatchComper>(num_compers){}

    virtual VertexT* toVertex(char* line)
    {
        VertexT* v = new VertexT;
        char * pch;
        pch = strtok(line, " \t");
        v->id = atoi(pch);
        pch = strtok(NULL, " \t");
        v->value.l = *pch;
        vector<AdjItem> & nbs = v->value.adj;
        AdjItem temp;
        while((pch=strtok(NULL, " ")) != NULL)
        {
        	temp.id = atoi(pch);
        	pch = strtok(NULL, " ");
        	temp.l = *pch;
        	nbs.push_back(temp);
        }
        return v;
    }

    virtual void task_spawn(VertexT * v, vector<GMatchTask> & tcollector)
	{
    	if(v->value.l != 'a') return;
    	GMatchTask t;
    	addNode(t.subG, *v);
    	t.context = 1;
		vector<AdjItem> & nbs = v->value.adj;
		bool has_b = false;
		bool has_c = false;
		for(int i=0; i<nbs.size(); i++)
			if(nbs[i].l == 'b')
			{
				t.pull(nbs[i].id);
				has_b = true;
			}
			else if(nbs[i].l == 'c')
			{
				t.pull(nbs[i].id);
				has_c = true;
			}
		if(has_b && has_c) tcollector.push_back(t);
	}
};

int main(int argc, char* argv[])
{
    init_worker(&argc, &argv);
    WorkerParams param;
    param.input_path = argv[1];  //input path in HDFS
    int thread_num = atoi(argv[2]);  //number of threads per process
    param.force_write=true;
    param.native_dispatcher=false;
    //------
	GMatchTrimmer trimmer;
    GMatchAgg aggregator;
    GMatchWorker worker(thread_num);
	worker.setTrimmer(&trimmer);
    worker.setAggregator(&aggregator);
    worker.run(param);
    worker_finalize();
    return 0;
}
