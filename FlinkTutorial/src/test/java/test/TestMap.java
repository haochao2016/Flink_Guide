package test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class TestMap {
    public static void main(String[] args) {

        HashMap<String, String> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            map.put("s" + i, "v" + i);
            String.valueOf(i);
        }

        System.out.println(map.get("s4"));


//        System.out.println(Integer.bitCount(12));
        /*try {
            TimeUnit.SECONDS.sleep(2);
            TimeUnit hours = TimeUnit.HOURS;
            hours.toHours(23);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }
}
