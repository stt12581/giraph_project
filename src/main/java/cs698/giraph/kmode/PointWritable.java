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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable {

	private int [] data = new int[0];
	
	public PointWritable() {
		this.data = new int[0];
	}
	
	public PointWritable(int [] data) {
		this.data = data;
	}
	
	public int[] getData() {
		return data;
	}

	public void setData(int[] data) {
		this.data = data;
	}

	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		if(data.length != length) {
			data = new int[length];
		}
		for(int i = 0; i < data.length; i++) {
			data[i] = in.readInt();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(data.length);
		for(int i = 0; i < data.length; i++) {
			out.writeInt(data[i]);
		}
	}
	
	public int getDimensions() {
		return data.length;
	}
	
	public String toString() {
		return Arrays.toString(data);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(data);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PointWritable other = (PointWritable) obj;
		if (!Arrays.equals(data, other.data))
			return false;
		return true;
	}
	
	
}
