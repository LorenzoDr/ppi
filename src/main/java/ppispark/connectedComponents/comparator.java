package experiments;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class comparator implements Comparator<Tuple2<Long,Integer>>,Serializable {

	@Override
	public int compare(Tuple2<Long, Integer> t1, Tuple2<Long, Integer> t2) {
		// TODO Auto-generated method stub
		if(t1._2>=t2._2) {
			return 1;
		}
		return -1;
	}

}
