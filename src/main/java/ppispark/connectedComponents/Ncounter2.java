package ppispark.connectedComponents;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Ncounter2 implements PairFunction<Tuple2<Object,Object>, Long, Integer> {
    private ArrayList<Object> N;
    public Ncounter2(ArrayList<Object> N) {
        this.N=N;
    }

    @Override
    public Tuple2<Long, Integer> call(Tuple2<Object, Object> t) throws Exception {
        // TODO Auto-generated method stub
        if(N.contains(t._2)) {
            return new Tuple2<Long,Integer>(Long.parseLong(t._1.toString()),1);
        }
        else {
            return new Tuple2<Long,Integer>(Long.parseLong(t._1.toString()),0);
        }
    }

}