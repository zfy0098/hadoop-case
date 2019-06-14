package com.hadoop.online;

import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/6/5.
 *
 * @author Zhoufy
 */
public class OnlineWritable implements WritableComparable<OnlineWritable> {


    private int appID;
    private int childID;
    private int channelID;
    private int appChannelID;
    private int packageID;
    private String uid;


    @Override
    public int compareTo(OnlineWritable o) {
        int compare;
        compare = appID - o.appID;
        if (compare != 0) {
            return compare;
        }
        compare = childID - o.childID;
        if (compare != 0) {
            return compare;
        }
        compare = channelID - o.channelID;
        if (compare != 0) {
            return compare;
        }
        compare = appChannelID - o.appChannelID;
        if (compare != 0) {
            return compare;
        }
        compare = packageID - o.packageID;
        if (compare != 0) {
            return compare;
        }
        compare = uid.compareTo(o.uid);
        if (compare != 0) {
            return compare;
        }
        return compare;

    }


    @Override
    public void readFields(DataInput in) throws IOException {
        this.appID = in.readInt();
        this.childID = in.readInt();
        this.channelID = in.readInt();
        this.appChannelID = in.readInt();
        this.packageID = in.readInt();
        this.uid = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(appID);
        out.writeInt(childID);
        out.writeInt(channelID);
        out.writeInt(appChannelID);
        out.writeInt(packageID);
        out.writeUTF(uid);
    }

    public OnlineWritable() {
    }

    public OnlineWritable(int appID, int childID, int channelID, int appChannelID, int packageID, String uid) {
        this.appID = appID;
        this.childID = childID;
        this.channelID = channelID;
        this.appChannelID = appChannelID;
        this.packageID = packageID;
        this.uid = uid;
    }

    public int getAppID() {
        return appID;
    }

    public void setAppID(int appID) {
        this.appID = appID;
    }

    public int getChildID() {
        return childID;
    }

    public void setChildID(int childID) {
        this.childID = childID;
    }

    public int getChannelID() {
        return channelID;
    }

    public void setChannelID(int channelID) {
        this.channelID = channelID;
    }

    public int getAppChannelID() {
        return appChannelID;
    }

    public void setAppChannelID(int appChannelID) {
        this.appChannelID = appChannelID;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getPackageID() {
        return packageID;
    }

    public void setPackageID(int packageID) {
        this.packageID = packageID;
    }

    @Override
    public int hashCode() {
        StringBuffer sbf = new StringBuffer();
        sbf.append(appID).append(childID).append(channelID).append(appChannelID).append(packageID).append(uid);
        return sbf.toString().hashCode() * 31;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof OnlineWritable)) {
            return false;
        } else {
            OnlineWritable r = (OnlineWritable) obj;
            if (this.appID == r.appID && this.channelID == r.channelID && this.childID == r.childID &&
                    this.appChannelID == r.appChannelID && this.packageID == r.packageID && this.uid.equals(r.getUid())) {
                return true;
            } else {
                return false;
            }
        }
    }


    @Override
    public String toString() {
        return appID +
                "\t" + childID +
                "\t" + channelID +
                "\t" + appChannelID +
                "\t" + packageID +
                "\t" + uid;
    }
}
