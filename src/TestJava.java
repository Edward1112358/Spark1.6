/**
 * Created by Edward on 2017-6-9.
 */
public class TestJava {
    public static void main(String[] args) {
        String str = "123";
        if (str.matches("\\d+")) {
            System.out.println(Integer.parseInt(str)+1);
        } else {
            System.out.println("不是数字");
        }
    }
}
