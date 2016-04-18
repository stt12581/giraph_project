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
import java.util.ArrayList;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class InputFormat extends TextVertexInputFormat<LongWritable, NodeState, NullWritable>{

	@Override
	public TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new ComponentisationVertexReader();
	}

	public class ComponentisationVertexReader extends TextVertexReader {

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}

		private long id;

		@Override
		public Vertex<LongWritable, NodeState, NullWritable> getCurrentVertex() throws IOException, InterruptedException {

			Text line = getRecordReader().getCurrentValue();
			String[] parts = line.toString().split(",");
			if(parts.length == 35) {
				int [] data = new int[parts.length];
				for(int i = 0; i< data.length; i++) {
					data[i] = Integer.parseInt(parts[i]);
				}
				LongWritable lwid = new LongWritable(id++);
				NodeState value = new NodeState(new PointWritable(data));

				ArrayList<Edge<LongWritable, NullWritable>> edgeIdList = new ArrayList<Edge<LongWritable, NullWritable>>();
				Vertex<LongWritable, NodeState, NullWritable> vertex = getConf().createVertex();

				vertex.initialize(lwid, value, edgeIdList);
				return vertex;
			} else {
				return null;
			}

		}
	}

}
