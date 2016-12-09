package com.octopx.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

/**
 * Created by yuyang on 16/7/1.
 */
public class DefineWritable {
    public static void main(String[] args) throws IOException {
        Student student = new Student("zhangsan", 18, "男");
        BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(new File("yang.txt")));
        DataOutputStream dos = new DataOutputStream(bos);
        student.write(dos);
        dos.flush();
        dos.close();
        bos.close();

        Student s = new Student();
        BufferedInputStream bis = new BufferedInputStream(
                new FileInputStream(new File("yang.txt")));
        DataInputStream dis = new DataInputStream(bis);
        s.readFields(dis);
        System.out.println("name = " + s.getName() + ", age = " + s.getAge() + ", sex=" + s.getSex());
    }
}

class Student implements WritableComparable<Student> {
    private Text name = new Text();
    private IntWritable age = new IntWritable();
    private Text sex = new Text();

    public Student() {
    }

    public Student(String name, int age, String sex) {
        this.name = new Text(name);
        this.age = new IntWritable(age);
        this.sex = new Text(sex);
    }

    @Override
    public int compareTo(Student o) {
        int result = 0;
        if ((result = this.name.compareTo(o.getName())) != 0) {
            return result;
        }
        if ((result = this.age.compareTo(o.getAge())) != 0) {
            return result;
        }
        if ((result = this.sex.compareTo(o.getSex())) != 0) {
            return result;
        }
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        age.write(out);
        sex.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        age.readFields(in);
        sex.readFields(in);
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public IntWritable getAge() {
        return age;
    }

    public void setAge(IntWritable age) {
        this.age = age;
    }

    public Text getSex() {
        return sex;
    }

    public void setSex(Text sex) {
        this.sex = sex;
    }
}