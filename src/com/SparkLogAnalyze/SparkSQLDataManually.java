package com.SparkLogAnalyze;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * Created by Edward on 2017-6-9.
 */
public class SparkSQLDataManually {

    static String[] channelNames = new String[]{"Spark", "Scala", "Kafka", "Flink", "Hadoop", "Storm"};
    static String[] actionNames = new String[]{"View", "Register"};
    static String yesterdayFormatted;

    public static void main(String[] args) {

        int numberItems = 5000;
        String path = ".";
        if (args.length > 0) {
            numberItems = Integer.parseInt(args[0]);
            path = args[1];
        }
        // System.out.println("User log number is: " + numberItems);

        yesterdayFormatted = yesterday();
        userLogs(numberItems, path);
    }

    public static void userLogs(int numberItems, String path) {
        StringBuffer userLogBuffer = new StringBuffer();
        Random random = new Random();
        for (int i = 0; i < numberItems; i++) {
            long timestamp = new Date().getTime();
            long userID = random.nextInt(numberItems);
            long pageID = random.nextInt(numberItems);
            String channel = channelNames[random.nextInt(channelNames.length)];
            String action = actionNames[random.nextInt(actionNames.length)];
            userLogBuffer.append(yesterdayFormatted).append("\t")
                    .append(timestamp).append("\t")
                    .append(userID).append("\t")
                    .append(pageID).append("\t")
                    .append(channel).append("\t")
                    .append(action).append("\n");
        }
        // System.out.print(userLogBuffer.toString());
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path)));
            printWriter.write(userLogBuffer.toString() + "userLog.log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            printWriter.close();
        }
    }


    public static String yesterday() {
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);
        Date yesterday = cal.getTime();
        return date.format(yesterday);
    }
}
