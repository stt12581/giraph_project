
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class NodeState implements Writable {
	
	private PointWritable point = new PointWritable();
	private int cluster = -1;
	private PointWritable clusterCentre = new PointWritable();
	
	public NodeState() {}
	
	public NodeState(PointWritable point) {
		this.point = new PointWritable(point.getData());
	}

	public PointWritable getPoint() {
		return point;
	}

	public void setPoint(PointWritable point) {
		this.point = new PointWritable(point.getData());
	}

	public int getCluster() {
		return cluster;
	}

	public void setCluster(int cluster) {
		this.cluster = cluster;
	}

	public PointWritable getClusterCentre() {
		return clusterCentre;
	}

	public void setClusterCentre(PointWritable clusterCentre) {
		this.clusterCentre = new PointWritable(clusterCentre.getData());
	}

	public void readFields(DataInput in) throws IOException {
		point.readFields(in);
		cluster = in.readInt();
		clusterCentre.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		point.write(out);
		out.writeInt(cluster);
		point.write(out);
	}
	
	public String toString() {
		return point + " is part of cluster " + clusterCentre;
	}
}
