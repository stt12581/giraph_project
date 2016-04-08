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

package cs698.giraph.kmode;

import org.apache.giraph.aggregators.Aggregator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

public class AveragePointWritableAggregator implements Aggregator<PointWritable> {

	private PointWritable majority = new PointWritable();
	//private int count = 0;
	private List<Map<Double, Integer>> dataMap = new ArrayList<Map<Double, Integer>>();
	
	public void aggregate(PointWritable value) {
		if(dataMap.size() == 0) {
			majority.setData(new double[value.getDimensions()]);
			for(int i=0; i<value.getDimensions(); i++){
				Map<Double,Integer> subMap = new HashMap<Double, Integer>();
    			dataMap.add(i,subMap);
    		}
		}
		if(value.getDimensions() == 0) {
			return;
		}

		int i = 0;
		for(Map<Double, Integer> map : dataMap) {
			//sum.getData()[i] = sum.getData()[i] + value.getData()[i];
			Integer key = map.get(value.getData()[i]);
			if(key!=null){
				map.put(value.getData()[i], (int)key + 1);
			} else{
				map.put(value.getData()[i], 1);
			}
			i++;
		}
		//count++;
	}

	public PointWritable createInitialValue() {
		return new PointWritable();
	}

	public PointWritable getAggregatedValue() {
		double [] data = new double[dataMap.size()];
		int i=0;
		for(Map<Double, Integer> map : dataMap) {
			int max = -1;
			double dimension = -1;

			Iterator it = map.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        if((int)pair.getValue() > max){
		        	max = (int)pair.getValue();
		        	dimension = (double)pair.getKey();
		        }
		        it.remove(); // avoids a ConcurrentModificationException
		    }
		    data[i] = dimension;
		    i++;
		}
		majority.setData(data.clone());
		return new PointWritable(data);
	}

	public void setAggregatedValue(PointWritable value) {//double check!
		int dimension = 2;
		if(value == null){
			majority.setData(new double[dimension]);
			for(int i=0; i < dimension; i++){
				majority.getData()[i] = Double.MIN_VALUE;
			}
		}
		else{
			majority.setData(value.getData().clone());
			int i=0;
			for(Map<Double, Integer> map : dataMap){
				map.put(value.getData()[i], 1);
				i++;
			}
		}
	}

	public void reset() {
		for(Map<Double, Integer> map : dataMap){
			map.clear();
		}
		majority.setData(new double[dataMap.size()]);
	}

}
