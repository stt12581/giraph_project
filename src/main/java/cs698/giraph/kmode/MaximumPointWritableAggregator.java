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

/*
 * Change the implementation from k-means to k-mode algorithm based on https://github.com/paulmw/giraph-examples
 */

package cs698.giraph.kmode;

import org.apache.giraph.aggregators.Aggregator;

public class MaximumPointWritableAggregator implements Aggregator<PointWritable> {

	private PointWritable maximum = new PointWritable();
	private int dimension = 35;
	
	public void aggregate(PointWritable value) {
		if(maximum.getDimensions() == 0) {
			maximum.setData(new int[value.getDimensions()]);
		}
		if(value.getDimensions() == 0) {
			return;
		}
		for(int i = 0; i < value.getDimensions(); i++) {
			maximum.getData()[i] = Math.max(maximum.getData()[i], value.getData()[i]);
		}
	}

	public PointWritable createInitialValue() {
		return new PointWritable();
	}

	public PointWritable getAggregatedValue() {
		return new PointWritable(maximum.getData());
	}

	public void setAggregatedValue(PointWritable value) {
		if(value == null){
			maximum.setData(new int[dimension]);
			for(int i=0; i < dimension; i++){
				maximum.getData()[i] = Integer.MAX_VALUE;
			}
		}
		else maximum.setData(value.getData().clone());
	}

	public void reset() {
		maximum.setData(new int[0]);
	}
	
}
