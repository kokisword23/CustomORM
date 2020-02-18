package entities;

import annotations.Column;
import annotations.Entity;
import annotations.Id;

import java.util.Date;

@Entity(name = "users")
public class User {

    @Id
    @Column(name ="id")
    private int id;

    @Column(name ="username")
    private String username;

    @Column(name ="password")
    private String password;

    @Column(name ="age")
    private int age;

    @Column(name ="registration_date")
    private Date registrationDate;

    @Column(name = "salary")
    private String salary;

    public User() {
    }

    public User(String username, String password, int age, Date registrationDate, String salary) {
        this.username = username;
        this.password = password;
        this.age = age;
        this.registrationDate = registrationDate;
        this.salary = salary;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Date getRegistrationDate() {
        return registrationDate;
    }

    public void setRegistrationDate(Date registrationDate) {
        this.registrationDate = registrationDate;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }
}
