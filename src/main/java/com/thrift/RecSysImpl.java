package com.thrift;

import org.apache.thrift.TException;

/**
 * Created with IDEA by ChouFy on 2019/5/30.
 *
 * @author Zhoufy
 */
public class RecSysImpl implements RecSys.Iface {

    public RecSysImpl() {
    }

    @Override
    public String rec_data(String username) throws TException {
        System.out.println("Hi," + username + "Welcome to my blog http://www.cnblogs.com/zfygiser");
        return "Hi," + username + "Welcome to my blog http://www.cnblogs.com/zfygiser";
    }
}
