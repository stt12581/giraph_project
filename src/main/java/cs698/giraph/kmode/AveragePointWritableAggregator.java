/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sa.giraph.examples.kmeans;

import org.apache.giraph.aggregators.Aggregator;
import java.util.List;
import java.util.Map;

public class AveragePointWritableAggregator implements Aggregator<PointWritable> {

	private PointWritable majority = new PointWritable();
	//private int count = 0;
	private List<Map<Integer, Integer>> dataMap = new ArrayList<Map<Integer, Integer>>();
	
	public void aggregate(PointWritable value) {
		if(dataMap.size() == 0) {
			majority.setData(new double[value.getDimensions()]);
			for(int i=0; i<value.getDimensions(); i++){
				Map<Integer,Integer> subMap = new HashMap<Integer, Integer>();
    			dataMap.add(i,subMap);
    		}
		}
		if(value.getDimensions() == 0) {
			return;
		}

		for(int i = 0; i < value.getDimensions(); i++) {
			//sum.getData()[i] = sum.getData()[i] + value.getData()[i];
			Integer key = dataMap[i].get(value.getData()[i]);
			if(key!=null){
				dataMap.put(key, dataMap.get(key) + 1);
			} else{
				dataMap[i].put(value.getData()[i], 1);
			}
		}
		//count++;
	}

	public PointWritable createInitialValue() {
		return new PointWritable();
	}

	public PointWritable getAggregatedValue() {
		double [] data = new double[dataMap.size()];
		for(int i = 0; i < dataMap.size(); i++) {
			int max = -1, dimension = -1;

			Iterator it = dataMap[i].entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        if(pair.getValue() > max){
		        	max = pair.getValue();
		        	dimension = pair.getKey();
		        }
		        it.remove(); // avoids a ConcurrentModificationException
		    }
		    data[i] = dimension;
		}
		majority.setData(data.clone());
		return new PointWritable(data);
	}

	public void setAggregatedValue(PointWritable value) {//double check!
		majority.setData(value.getData().clone());
		for(int i=0; i<dataMap.size(); i++){
			dataMap[i].put(value.getData()[i], 1);
		}
	}

	public void reset() {
		for(int i=0; i<dataMap.size(); i++){
			dataMap[i].clear();
		}
		majority.setData(new double[dataMap.size()]);
	}

}
