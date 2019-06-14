package com.hadoop.online;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/5/30.
 *
 * @author Zhoufy
 */
public class OnlineMap extends Mapper<LongWritable, Text, OnlineWritable, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        JSONObject json = JSONObject.parseObject(line);
        String uid = json.getString("uToken");

        // int appID, int childID, int channelID, int appChannelID, String uid

//        {"roleLevel":"25","imei":"865166028949876","api_ver":"1.4.1","app_ver":"1.0.8","partyRoleID":"0","taskName":"","roleCTime":"1556611174",
// "roleLevelIMTime":"0","honorId":"","format":"json","t":1556640002,"professionID":"0","serverID":"6",
// "moneyNum":"870","channel_id":"59","honorName":"","serverName":"S6.阿特流姆","ic":"track","source":"59",
// "sign":"3682642e3fc06552562d7a0206e2feed","roleName":"威仪s火帝","roleID":"60171366","friendList":"无",
// "package_id":"507","vip":"0","cat":"online","taskId":"","appid":"10090","acid":"268","partyRoleName":"无",
// "device_name":"redmi note 3","device_os_ver":"Android 5.1.1","sdk_ver":"1.4.1","taskStatus":"","app_ver_code":
// "108","os":"1","power":"2693","partyID":"0","profession":"无","partyName":"null","gender":"无","child_id":"36","
// uToken":"1123056694084243457"}

        OnlineWritable onlineWritable = new OnlineWritable(json.getInteger("appid"),
        json.getInteger("child_id"),json.getInteger("channel_id"),json.getInteger("acid"), json.getInteger("package_id"), uid );

        context.write(onlineWritable, value);
    }
}
