package ap.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point implements Writable {

	public int x;
	public int y;
	public float sim;
	public float avail;
	public float res;
	
	public Point(){}
	
	public Point(int x,int y,float sim,float avail,float res){
		this.x=x;
		this.y=y;
		this.sim=sim;
		this.avail=avail;
		this.res=res;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		x=in.readInt();
		y=in.readInt();
		sim=in.readFloat();
		avail=in.readFloat();
		res=in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(x);
		out.writeInt(y);
		out.writeFloat(sim);
		out.writeFloat(avail);
		out.writeFloat(res);
	}
	
	public Point like(){
		Point pt=new Point();
		pt.x=this.x;
		pt.y=this.y;
		pt.sim=this.sim;
		pt.avail=this.avail;
		pt.res=this.res;
		return pt;
	}
	
	public int hashCode(){
		return x*10+y;
	}
	
	public boolean equals(Object o){
		if(o instanceof Point){
			Point pt=(Point)o;
			return x==pt.x&&y==pt.y;
		}
		return false;
	}
	
	public String toString(){
	    StringBuilder result = new StringBuilder();
	    result.append(x);
	    result.append(",");
	    result.append(y);
	    result.append(" ");
	    result.append(sim);
	    result.append(",");
	    result.append(avail);
	    result.append(",");
	    result.append(res);
	    return result.toString();
	}

}
