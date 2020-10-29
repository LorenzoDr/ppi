package ppispark.util;

import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Ncounter implements PairFunction<Tuple2<Long,String>, Long, Integer> {
	private List<String> N;
	public Ncounter(List<String> N) {
		this.N=N;
	}
	
	@Override
	public Tuple2<Long, Integer> call(Tuple2<Long, String> t) throws Exception {
		// TODO Auto-generated method stub
		if(N.contains(t._2)) {
			return new Tuple2<Long,Integer>(t._1,1);
		}
		else {
			return new Tuple2<Long,Integer>(t._1,0);
		}
	}

}
