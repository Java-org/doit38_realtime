package cn.doitedu.demo10.groovy_demo

class PersonGroovy implements Person{

    int age;
    String name;

    void set(int age,String name){
        this.age = age;
        this.name = name;
    }

    void sayHello(String who){
        println(who + ", welcome! i am " + this.name +", i am " +this.age + " yeas old!")
    }

}
