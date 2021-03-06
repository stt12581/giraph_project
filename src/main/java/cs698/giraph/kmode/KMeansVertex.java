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

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.conf.LongConfOption;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
/*
 * This algorithm implements K-Means clustering. Each data point in the dataset becomes a vertex 
 * in the graph, and cluster centres are computed using custom aggregators.
 */
public class KMeansVertex extends BasicComputation<
    LongWritable, NodeState, NullWritable, LongWritable>{
        private static final Logger LOG = Logger.getLogger(KMeansVertex.class);	
	private final static LongWritable one = new LongWritable(1);
	public static final LongConfOption K =
      new LongConfOption("KMeansVertex.k", 1, "K Means");
	
	@Override
	public void compute(Vertex<LongWritable, NodeState, NullWritable> vertex, 
			Iterable<LongWritable> messages) throws IOException {
		// In the first superstep, we compute the ranges of the dimensions 
		if(getSuperstep() == 0) {
			aggregate(Constants.MAX, vertex.getValue().getPoint());
			aggregate(Constants.MIN, vertex.getValue().getPoint());
			return;
		} else {
			
			// If there were no cluster reassignments in the previous superstep, we're done.
			// (Other stopping criteria (not implemented here) could include a fixed number of
			// iterations, cluster centres that are not moving, or the Residual Sum of Squares
			// (RSS) is below a certain threshold.
			if(getSuperstep() > 1) {
				LongWritable updates = getAggregatedValue(Constants.UPDATES);
				if(updates.get() == 0) {
					vertex.voteToHalt();
					return;
				}
			}
			
			// If we're not stopping, we need to compute the closest cluster to this node
			int k = (int)K.get(getConf());
			PointWritable [] means = new PointWritable[k];
			int closest = -1;
			int closestDistance = Integer.MAX_VALUE;
			for(int i = 0; i < k; i++) {
				means[i] = getAggregatedValue(Constants.POINT_PREFIX + i);
				int d = distance(vertex.getValue().getPoint().getData(), means[i].getData());
				if(d < closestDistance) {
					closestDistance = d;
					closest = i;
				}
			}
			
			// If the choice of cluster has changed, aggregate an update so the we recompute
			// on the next iteration.
			if(closest != vertex.getValue().getCluster()) {
				aggregate(Constants.UPDATES, one);
			}
			
			// Ensure that the closest cluster position is updated, irrespective of whether or
			// not the choice of cluster has changed.
			NodeState state = vertex.getValue();
			state.setCluster(closest);
			state.setClusterCentre(means[closest]);
			vertex.setValue(state);
			
			// Prepare the next iteration by aggregating this point into the closest cluster.
			aggregate(Constants.POINT_PREFIX + closest, vertex.getValue().getPoint());
		}

	}
	
	/*
	 * Hamming distance
	 */
	private int distance(int [] a, int [] b) {
		if(a.length == 0 || b.length == 0) {
			return Integer.MAX_VALUE;
		}
		if(a.length != b.length) {
			throw new IllegalArgumentException();
		}
		int result = 0;
		for(int i = 0; i < a.length; i++) {
			result += a[i]==b[i] ? 0:1;
		}
		return result;
	}
	
}
