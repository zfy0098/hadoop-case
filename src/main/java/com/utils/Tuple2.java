package com.utils;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created with IDEA by ChouFy on 2019/7/23.
 *
 * @author Zhoufy
 */
public class Tuple2<T1, T2> implements Writable ,Comparable<Tuple2<T1, T2>>, Serializable {
    public final T1 _1;
    public final T2 _2;

    public static <A, B> Tuple2<A, B> T2(A a, B b) {
        return new Tuple2<A, B>(a, b);
    }

    public Tuple2(T1 first,
                  T2 second) {
        this._1 = first;
        this._2 = second;
    }

    public T1 first() {
        return _1;
    }

    public T2 second() {
        return _2;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        return hcb.append(_1).append(_2).toHashCode();
    }

    public boolean equals(Tuple2<T1, T2> other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        return (_1 == other._1 || (_1 != null && _1.equals(other._1)))
                && (_2 == other._2 || (_2 != null && _2.equals(other._2)));
    }

    @Override
    public int compareTo(Tuple2<T1, T2> other) {
        if (this == other) {
            return 0;
        }
        if (other == null) {
            return -1;
        }
        if (getClass() != other.getClass()) {
            return -1;
        }
        if ((_1 == other._1) ||
                (_1 != null && _1.equals(other._1))
        ) {
            return 0;
        }
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Tuple3[");
        sb.append(_1).append(",").append(_2).append(",");
        return sb.append("]").toString();
    }
}
