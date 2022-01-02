// Databricks notebook source

object Hello {
    def main(args: Array[String]) = {
        println("Hello world")
    }
}
/**
  * Created by scouto.
  */
object Intro extends App {
  println("Hello world from Intro")


  //Ejemplos funciones
  def sum(x: Int, y: Int): Int = {
    x + y
  }

  //Ejemplos funciones Parciales
  def divide: PartialFunction[Int, Int] = {
    case d: Int if d != 0 => 42 / d
  }

  val divide2: PartialFunction[Int, Int] = {
    case d: Int if d != 0 => 42 / d
  }

  def divide3(x: Int): PartialFunction[Int, Int] = {
    case d: Int if d != 0 => x / d
  }


}

// COMMAND ----------

Hello.main(Array())
Intro.main(Array())

// COMMAND ----------

//Ejercicio 1: substract, mutiply, divide

def subs = ???
def mult = ???
def div = ???

// COMMAND ----------

//Ejercicio

//def operate


// COMMAND ----------

import Intro._
import org.scalatest._

class IntroTest extends FlatSpec with Matchers {

  "sum" should "work with both natural numbers" in {
    sum(2,3) should be (5)
  }
  
  /*
  "subs" should "work with both natural numbers" in {
    subs(2,1) should be (1)
    subs(3,5) should be (-2)
  }
  
  it should "work with 0" in {
    subs(2,0) should be (2)
    subs(0,5) should be (-5)
  }
  
  it should "work with negative numbers" in {
    subs(-2,-1) should be (-1)
  }
  
  "mult" should "work with both natural numbers" in {
    mult(2,0) should be (0)
    mult(1,3) should be (3)
    mult(3,5) should be (15)
  }

  it should "work with negative numbers" in {
    mult(-2,-2) should be (4)
    mult(2,-5) should be (-10)
  }
  
 "operate" should "work with sum" in {
    operate(2, 3, (x: Int, y: Int) => x+ y) should be (sum(2,3))
    operate(3, 3, sum) should be (sum(3,3))
  }*/

}

(new IntroTest).execute()

