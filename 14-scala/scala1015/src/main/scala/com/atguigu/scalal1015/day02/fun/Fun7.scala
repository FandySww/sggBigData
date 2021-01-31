package com.atguigu.scalal1015.day02.fun

object Fun7 {
  def main(args: Array[String]): Unit = {
    val f1 = foo
    f1()
  }
  def foo() = {
    def f() = {
      printf("aaaaaaa")
    }
    f _
  }
}
