package test;

import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 *  update the Final variable through reflect
 */
public class TestUpdateFinal {
 
	public static void main(String[] args) {
		OneCity oneCity = new OneCity();
		System.out.println("反射前：" + oneCity.getValue());
		try {
			Field nameField;
			nameField = oneCity.getClass().getDeclaredField("names");
			nameField.setAccessible(true); // 这个同样不能少，除非上面把 private 也拿掉了，可能还得 public
			ArrayList<String> other = new ArrayList<>();
			other.add("world");
			nameField.set(oneCity, other);
//			nameField.set(oneCity, "other");
			System.out.println("反射后：" + oneCity.getValue());
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 
	}
}

class OneCity {
	private final ArrayList<String> names;
//	private final String names;

	public OneCity() {
		names = new ArrayList<>();
		names.add("hello");
//		names = "hello";
	}

	public  String getValue() {
		return names.toString();
	}
}



