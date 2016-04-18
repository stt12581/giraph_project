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

import java.util.Random;
import org.apache.log4j.Logger;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

public class MasterCompute extends DefaultMasterCompute {

	private int k;
	private Random r;
	private static final Logger LOG = Logger.getLogger(MasterCompute.class);
	public static final LongConfOption K =
      new LongConfOption("KMeansVertex.k", 1, "K Means");
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		r = new Random();
		registerPersistentAggregator(Constants.MIN, MinimumPointWritableAggregator.class);
		registerPersistentAggregator(Constants.MAX, MaximumPointWritableAggregator.class);
		registerAggregator(Constants.UPDATES, LongSumAggregator.class);
		//k = getConf().getInt(Constants.K, 2);
		k = (int)K.get(getConf());
		
		for(int i = 0; i < k; i++) {
			registerAggregator(Constants.POINT_PREFIX + i, AveragePointWritableAggregator.class);
		}
	}

	@Override
	public void compute() {
		if(getSuperstep() == 1) {
			PointWritable min = getAggregatedValue(Constants.MIN);
			PointWritable max = getAggregatedValue(Constants.MAX);
			for(int i = 0; i < k; i++) {
				PointWritable p = random();
				setAggregatedValue(Constants.POINT_PREFIX + i, p);
			}
		}
		LOG.info("Superstep: " + getSuperstep());
	}
	
	private PointWritable random() {
		int length = 35;
		int [] randomData = new int[length];
		for(int i=0; i<length; i++){
			if(i==0) randomData[i] = r.nextInt(7);
			else if(i==1||i==4||i==8||i==11||i==19||i==22||i==23||i==24||i==26||i==34)
				randomData[i] = r.nextInt(2);
			else if(i==2||i==3||i==9) randomData[i] = r.nextInt(3);
			else if(i==5||i==6||i==20||i==21) randomData[i] = r.nextInt(4);
			else if(i==7) randomData[i] = r.nextInt(2)+1;
			else if(i==10||i==18) randomData[i] = 1; 
			else if(i==13||i==14) randomData[i] = 2;
			else if(i==25||i==27){
				randomData[i] = r.nextInt(2);
				if(randomData[i] == 1){
					if(i==25) randomData[i] = 2;
					else randomData[i] = 3;
				}
			}
			else if(i==28) randomData[i] = 4;
			else randomData[i] = 0;
		}
		return new PointWritable(randomData);
	}

	private int random(int min, int max) {
		int x = r.nextInt(7);
		return x;
	}
}
