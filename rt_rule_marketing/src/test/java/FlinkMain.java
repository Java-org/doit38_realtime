public class FlinkMain {

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {


        String code = "public class CalculatorX {\n" +
                "\n" +
                "    public void init(){\n" +
                "        System.out.println(\"我被初始化了\");\n" +
                "    }\n" +
                "\n" +
                "    public void calc(){\n" +
                "        System.out.println(\"我运行起来了\");\n" +
                "    }\n" +
                "\n" +
                "}\n";


        // 利用jdk的编译api，在程序运行中对上面的源代码进行编译

        // 编译完后，要做类加载，将编译好的class加载到运行时的classpath中

        // 接下来就可以利用反射api来实例化这个类了


    }

}
