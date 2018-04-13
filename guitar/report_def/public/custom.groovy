public class customdemo {
    public static final String VALUE_EMPTY = "EMPTY";

    public static boolean isEmpty(String str) {
        return null == str || str.trim().length() == 0 || VALUE_EMPTY.equals(str);
    }

}