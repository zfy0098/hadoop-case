package com.utils;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TupleComparator implements
        Comparator<Tuple2<String, Integer>>, Serializable {



    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        return 0;
    }
}
