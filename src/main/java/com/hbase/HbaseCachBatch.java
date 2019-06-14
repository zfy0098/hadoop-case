package com.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/6/10.
 *
 * @author Zhoufy
 */
public class HbaseCachBatch extends HbaseBase {


    private static void scan(int caching, int batch) throws IOException {
        Logger log = Logger.getLogger("org.apache.hadoop");
        final int[] counters = {0, 0};
        Appender appender = new AppenderSkeleton() {
            @Override
            protected void append(LoggingEvent event) {
                String msg = event.getMessage().toString();
                System.out.println("1111" + msg);
                if (msg != null && msg.contains("Call: next")) {
                    counters[0]++;
                }
            }

            @Override
            public void close() {

            }

            @Override
            public boolean requiresLayout() {
                return false;
            }
        };

        log.removeAllAppenders();
        log.setAdditivity(false);
        log.addAppender(appender);
        log.setLevel(Level.DEBUG);

        Scan scan = new Scan();
        scan.setCaching(caching);
        scan.setBatch(batch);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            counters[1]++;
        }

        scanner.close();

        System.out.println("Caching: " + caching + " , batch : " + batch +
                ", results :" + counters[1] + ", rpcs:" + counters[0]);

    }


    public static void main(String[] args) throws IOException{
        HbaseCachBatch.scan(1,1);

    }
}
