package cn.gv;

import groovy.lang.GroovyClassLoader;

public class JavaTest {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {

        String code = "package cn.gv\n" +
                "class CalculatorU implements Calculator{\n" +
                "    public void init(){\n" +
                "        System.out.println(\"我被初始化了\")\n" +
                "    }\n" +
                "    public void cacl(){\n" +
                "        System.out.println(\"我被运行了\")\n" +
                "    }\n" +
                "}";

        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        Class aClass = groovyClassLoader.parseClass(code);

        Calculator o = (Calculator) aClass.newInstance();
        o.init();
        o.cacl();


    }
}
