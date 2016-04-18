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
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

public class AveragePointWritableAggregator implements Aggregator<PointWritable> {
	private final Logger LOG = Logger.getLogger(AveragePointWritableAggregator.class);
	private PointWritable majority = new PointWritable();
	//private PointWritable sum = new PointWritable();
	//private int count = 0;
	private List<Map<Integer, Integer>> dataMap = new ArrayList<Map<Integer, Integer>>();
	
	public void aggregate(PointWritable value) {
		/*if(sum.getDimensions() == 0) {
			sum.setData(new double[value.getDimensions()]);
		}
		if(value.getDimensions() == 0) {
			return;
		}
		for(int i = 0; i < value.getDimensions(); i++) {
			sum.getData()[i] = sum.getData()[i] + value.getData()[i];
		}
		count++;*/
//LOG.info("value: " +value.getData()[0]+" "+value.getData()[1]);
//LOG.info("datamapsize: "+dataMap.size());
		if(dataMap.size() == 0) {
			majority.setData(new int[value.getDimensions()]);
			for(int i=0; i<value.getDimensions(); i++){
				Map<Integer,Integer> subMap = new HashMap<Integer, Integer>();
    			dataMap.add(subMap);
    		}
		}
		if(value.getDimensions() == 0) {
			return;
		}

		int i = 0;
		for(Map<Integer, Integer> map : dataMap) {
			//sum.getData()[i] = sum.getData()[i] + value.getData()[i];
			int d = value.getData()[i];
			if(map.containsKey(d)){
				map.put(d, map.get(d)+1);//map.get(d) + 1);
LOG.info("containsk: " + d+" "+map.get(d));
			} else{
				map.put(d, 1);
LOG.info("notcontainsk: " + d);
			}
			i++;
		}
		//count++;
	}

	public PointWritable createInitialValue() {
		return new PointWritable();
	}

	public PointWritable getAggregatedValue() {
		int [] data = new int[dataMap.size()];
		int i=0;
LOG.info("getAggr start: ");
		for(Map<Integer, Integer> map : dataMap) {
			int max = -1;
			int dimension = -1;

		    for(Map.Entry<Integer, Integer> pair : map.entrySet()) {
		        if((int)pair.getValue() > max){
		        	max = (int)pair.getValue();
		        	dimension = (int)pair.getKey();
		        }
			LOG.info("pair: " + pair.getKey()+" "+pair.getValue());
		    }
		    data[i] = dimension;
		    i++;
			LOG.info("dimension: " + max);
		}
LOG.info("getAggr stop: ");
		//LOG.info("max data: " + data[0]+" "+data[1]);
		majority.setData(data.clone());
		return new PointWritable(data);
		
		/*double [] data = sum.getData().clone();
		for(int i = 0; i < data.length; i++) {
			data[i] /= count;
		}
		return new PointWritable(data);*/
	}

	public void setAggregatedValue(PointWritable value) {//double check!
		int dimension = 35;
		//LOG.info("datamapsizepre: " + dataMap.get(0).size());
		if(value == null){
			majority.setData(new int[dimension]);
			for(int i=0; i < dimension; i++){
				majority.getData()[i] = Integer.MIN_VALUE;
			}
		}
		else{
		LOG.info("setAgg value is not null");
	//		reset();
			majority.setData(value.getData().clone());
		/*	int i=0;
			for(Map<Double, Integer> map : dataMap){
				double d = value.getData()[i];
				map.put(d, map.get(d)+1);
				i++;
			}*/
		}
	        //LOG.info("dataMapsizeafter: " + dataMap.get(0).size());	
		/*int dimension = 2;
		if(value == null){
			sum.setData(new double[dimension]);
			for(int i=0; i<dimension; i++){
				sum.getData()[i] = Double.MAX_VALUE;
			}
		} else{
			sum.setData(value.getData().clone());
			count = value.getDimensions() == 0 ? 0 : 1;
		}*/
	}

	public void reset() {
		LOG.info("Rest: ");
		for(Map<Integer, Integer> map : dataMap){
			map.clear();
		}
		majority.setData(new int[dataMap.size()]);
		
		//sum.setData(new double[0]);
		//count = 0;
	}

}
