package cn.doitedu.demo9.groovy_demo;

import groovy.lang.GroovyClassLoader;

public class JavaProgram {

    public static void main(String[] args) throws InstantiationException, IllegalAccessException {

        // 调用  类路径中已存在的groovy类
        PersonGroovy person = new PersonGroovy();
        person.set(18,"小岳岳");

        person.sayHello("涛总");



        String groovy2Code = "package cn.doitedu.demo9.groovy_demo \n" +
                "\n" +
                "class PersonGroovy2 implements Person {\n" +
                "\n" +
                "    int age;\n" +
                "    String name;\n" +
                "\n" +
                "    void set(int age,String name){\n" +
                "        this.age = age + 10;\n" +
                "        this.name = name;\n" +
                "    }\n" +
                "\n" +
                "    void sayHello(String who){\n" +
                "        println(who + \", nice to meet you! i am \" + this.name +\", i am \" +this.age + \" yeas old!\")\n" +
                "    }\n" +
                "\n" +
                "}\n";

        // 对源码进行动态编译、类加载
        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        Class aClass = groovyClassLoader.parseClass(groovy2Code);

        // 反射，实例化，调用
        Person person2 = (Person) aClass.newInstance();
        person2.set(29,"谦哥");
        person2.sayHello("刚哥");

    }
}
