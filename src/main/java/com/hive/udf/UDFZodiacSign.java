package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created with IDEA by ChouFy on 2019/5/29.
 *
 * @author Zhoufy
 */
public class UDFZodiacSign extends UDF {

    private SimpleDateFormat df;

    public UDFZodiacSign() {
        df = new SimpleDateFormat("yyyy-MM-dd");
    }

    public String evaluate(Date bday) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(bday);

        return evaluate(calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH));
    }

    public String evaluate(String bday) {
        Date date = null;
        Calendar calendar = Calendar.getInstance();
        try {
            date = df.parse(bday);
            calendar.setTime(date);
        } catch (Exception ex) {
            System.out.println("异常");
            ex.printStackTrace();
            return null;
        }
        return evaluate(calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH));
    }

    public String evaluate(Integer month, Integer day) {
        if (month == 1) {
            if (day < 20) {
                return "Capricorn";
            } else {
                return "Aquarius";
            }
        }
        if (month == 2) {
            if (day < 19) {
                return "Capricorn";
            } else {
                return "Pisces";
            }
        }
        if (month == 3) {
            if (day < 20) {
                return "Pisces";
            } else {
                return "Aries";
            }
        }
        if (month == 4) {
            if (day < 20) {
                return "Aries";
            } else {
                return "Taurus";
            }
        }
        if (month == 5) {
            if (day < 20) {
                return "Taurus";
            } else {
                return "Gemini";
            }
        }
        if (month == 6) {
            if (day < 21) {
                return "Gemini";
            } else {
                return "Cancer";
            }
        }
        if (month == 7) {
            if (day < 22) {
                return "Cancer";
            } else {
                return "Leo";
            }
        }
        if (month == 8) {
            if (day < 23) {
                return "Leo";
            } else {
                return "Virgo";
            }
        }
        if (month == 9) {
            if (day < 22) {
                return "Virgo";
            } else {
                return "Libra";
            }
        }
        if (month == 10) {
            if (day < 24) {
                return "Libra";
            } else {
                return "Scorpio";
            }
        }
        if (month == 11) {
            if (day < 22) {
                return "Scorpio";
            } else {
                return "Sagittarius";
            }
        }
        if (month == 12) {
            if (day < 22) {
                return "Sagittarius";
            } else {
                return "Capricorn";
            }
        }
        return null;
    }

}
