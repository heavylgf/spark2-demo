package cn.ctwl.spark

/**
 * Hello world!
 *
 */
object App {
//  // 实现经典的斐波那契数列：
  def fab(n:Int):Int = { 
     if(n <= 1) 1
     else fab(n-1) + fab(n-2)
  }
  
  // 函数入门之默认参数和带名参数
//  def sayHello(firstName: String, middleName: String = "William", lastName: String = "Croft")
//    = firstName + " " + middleName + " " + lastName 
    
  // Java与Scala实现默认参数的区别
  //Java：
  //public void sayHello(String name, int age) {
  //  if(name == null) {
  //    name = "defaultName"
  //  }
  //  if(age == 0) {
  //    age = 18
  //  }
  //}
  //sayHello(null, 0)

  // Scala：
//  def sayHello(name: String, age: Int = 20) {
//    print("Hello, " + name + ", your age is " + age)
//  }
//  sayHello("leo")
    
  // 带名参数
//  def sayHello(firstName:String = "Mick", lastName:String = "Nina", middleName:String = "Jack"){}
//  sayHello("Mick", lastName = "Nina", middleName = "Jack")
  
  // 函数入门之变长参数
  def sum(nums: Int*) = {
    var res = 0
    for (num <- nums) res += num
    res
  }
  sum(1, 2, 3, 4)




   

  
  def main(args: Array[String]): Unit = {
    // val声明之后不能改变
    val result = 1 + 1
    val myresult = 2 * result
    println("myresult：" + myresult)
    
    // 如果要声明值可以改变的引用，可以使用var变量
    var result1 = 1
    result1 = 2
    println("result1：" + result1)
    
    1.+(1)
    1.to(10)
    1 to 10
    var counter = 1
    counter += 1
    
    // 函数调用与apply()函数
    import scala.math._
    math.sqrt(2)
    math.pow(2, 4)
    math.min(3, 4)
    // 与java不同的一点是，如果调用函数时，不需要传递参数的话，则scala允许调用函数时省略括号的，
    // 例如，"Hello World".distinct
    
    // apply函数
    
    
    
    // 条件控制与循环
    val age = 30
    if(age > 18) 1 else 0
    // 可以将if表达式赋予一个变量
    val isAdult = if(age > 18) 1 else 0
    // if表达式的类型推断：由于if表达式是有值的，而if和else子句的值类型可能不同，此时if表达式的值是什么类型呢？
    // Scala会自动进行推断，取两个类型的公共父类型。
    // •例如，if(age > 18) 1 else 0，表达式的类型是Int，因为1和0都是Int
    // •例如，if(age > 18) "adult" else 0，此时if和else的值分别是String和Int，则表达式的值是Any，
    // Any是String和Int的公共父类型
    // •如果if后面没有跟else，则默认else的值是Unit，也用()表示，类似于java中的void或者null。例如，
    // val age = 12; if(age > 18) "adult"。此时就相当于if(age > 18) "adult" else ()。
    // •将if语句放在多行中：默认情况下，REPL只能解释一行语句，但是if表达式通常需要放在多行。
    // •可以使用{}的方式，比如以下方式，或者使用:paste和ctrl+D的方式。
//    if(age > 18) { "adult" 
//    } else if(age > 12) "teenager" else "children"

    // 循环
    // •while do循环：Scala有while do循环，基本语义与Java相同。
    var n = 10
    while(n > 0) {
      println(n)
      n -= 1
    }
    
    // 简易版for语句：
    var m = 10; for(i <- 1 to m) println(i)
    
    // 跳出循环语句
    // scala没有提供类似于java的break语句。
    // •但是可以使用boolean类型变量、return或者Breaks的break函数来替代使用
    import scala.util.control.Breaks._
    breakable {
        var n = 10
        for(c <- "Hello World") {
            if(n == 5) break;
            print(c)
            n -= 1
        }
    }    

    
    
    println("Hello World !!")
    println("刘光富  你好吗？？")
    println("ni haoma ?  ?")
  }
  
}
