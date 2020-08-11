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

#ifndef AGGSYNC_H_
#define AGGSYNC_H_

#include "util/global.h"
#include "util/communication.h"
#include <unistd.h> //for usleep()
#include <thread>
using namespace std;

template <class AggregatorT>
class AggSync {
public:
	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

	thread main_thread;

    /**
     * ������־ͬ����ֻҪ��һ�� worker �� endTag Ϊ true���򷵻� true
     */
	bool endTag_sync() //returns true if any worker has endTag = true
	{
		bool endTag = global_end_label;
		if(_my_rank != MASTER_RANK)
		{
            // �� master �� worker 
			send_data(endTag, MASTER_RANK, AGG_CHANNEL); // �� master ���Լ��Ľ�����־
			bool ret = recv_data<bool>(MASTER_RANK, AGG_CHANNEL); // ���� master �������Ľ�����־
			return ret;
		}
		else
		{
            // master worker
			bool all_end = endTag;
            // �������� worker �������Ľ�����־������Щ������־�� ���� ����������һ�� worker ������־Ϊ true�������� all_end ҲΪ true�����ؽ��ҲΪ true��
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) all_end = (recv_data<bool>(i, AGG_CHANNEL) || all_end);
			}

            // ������ worker �������յĽ�����־
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) send_data(all_end, i, AGG_CHANNEL);
			}
			return all_end;
		}
	}

	void agg_sync()
	{
		AggregatorT* agg = (AggregatorT*)global_aggregator;
		if (agg != NULL)
		{
			if (_my_rank != MASTER_RANK)
			{ 	//send partialT to aggregator
                // �� master worker ����ȡ�Լ��ڲ����� comper �ۺϺ�����ݣ����� part �У���Ȼ�󽫸ý������ master
				PartialT part;
				agg->finishPartial(part); // ��ȡ�ڲ��ۺϺ������
				send_data(part, MASTER_RANK, AGG_CHANNEL); // ���� master
				//scattering FinalT
				agg_rwlock.wrlock();
				*((FinalT*)global_agg) = recv_data<FinalT>(MASTER_RANK, AGG_CHANNEL); // ���� master �����������վۺϵ�����
				agg_rwlock.unlock();
			}
			else
			{
                // master worker
                // �������� worker �������ľֲ��ۺ����ݣ��������Ǿۺ���һ��
				for (int i = 0; i < _num_workers; i++)
				{
					if(i != MASTER_RANK)
					{
						PartialT part = recv_data<PartialT>(i, AGG_CHANNEL); // �������� worker �������ľֲ�����
						agg->stepFinal(part); // �����յ����ݽ��оۺ�
					}
				}

                // �� master �ڲ��ľۺ����������յ����ݽ��н��оۺ�
				FinalT final; // ���յľۺϽ��
				agg->finishFinal(final); // ���оۺ�
				//cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
				agg_rwlock.wrlock();
				*((FinalT*)global_agg) = final; //deep copy
				agg_rwlock.unlock();

                // �����յľۺϽ���������� worker
				for(int i=0; i<_num_workers; i++)
				{
					if(i != MASTER_RANK)
						send_data(final, i, AGG_CHANNEL);
				}
			}
		}
		//------ call agg UDF: init(prev)
		agg_rwlock.rdlock();
		agg->init_aggSync(*((FinalT*)global_agg)); // ��һ�־ۺ�ǰ���Ծۺ���������һ�εĳ�ʼ��
		agg_rwlock.unlock();
	}

    void run()
    {
    	while(true) //global_end_label is checked with "any_end"
    	{
    		bool any_end = endTag_sync();
    		if(any_end)
    		{
                // ������һ�� worker �Ľ�����־Ϊ true ʱ����������һ�� worker ���������������ٵ���  agg_sync ���оۺϣ����������ȴ����� worker ִ��������
    			while(global_end_label == false); //block till main_thread sets global_end_label = true
    			return;
    		}
    		else{
                // ���� worker �����ڹ���״̬�������ͬ��
    			agg_sync();
    			usleep(AGG_SYNC_TIME_GAP); //polling frequency �ۺ�һ�κ󣬿���һ��ʱ�䣬�ٽ�����һ�ξۺ�
    		}
    	}
    }

    AggSync()
    {
    	main_thread = thread(&AggSync<AggregatorT>::run, this);
    }

    ~AggSync()
    {
    	main_thread.join();
    	agg_sync(); //to make sure results of all tasks are aggregated �����پۺ���ʱ�ٽ������һ�ξۺϣ�ȷ�����н�����Ѿ��ۺ���һ��
    }
};

#endif
