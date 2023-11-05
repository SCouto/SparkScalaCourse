// Databricks notebook source
//AUX
def sum(x: Int, y: Int) = x +y
def prod(x: Double, y: Double) = x +y


 def sum(ints: List[Int]): Int = {
    ints match {
      case Nil => 0
      case h :: t => h + sum(t)
    }
  }

 def product(ints: List[Double]): Double = {
    ints match {
      case Nil => 1.0
      case h :: t => h * product(t)
    }
  }

// COMMAND ----------

 

//Ejercicios I
  def foldRight[A, B](as: List[A], z: B)(f: (A, B) => B): B = ???

  def sumFold(ints: List[Int]): Int = ???

  def productFold(lista: List[Double]): Double =  ???

  def lengthFoldRight[A](as: List[A]): Int = ???



// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


  def sum(ints: List[Int]): Int = {
    ints match {
      case Nil => 0
      case h :: t => h + sum(t)
    }
  }

 def product(ints: List[Double]): Double = {
    ints match {
      case Nil => 1.0
      case h :: t => h * product(t)
    }
  }

class Ejercicios1Test extends FlatSpec with Matchers with ScalaCheckPropertyChecks {
   "sumFold" should "work the same as sum" in {
    sumFold(List()) should be (sum(List()))
    sumFold(Nil) should be (sum(Nil))
    sumFold(List(1,2)) should be (sum(List(1,2)))
    sumFold(List(-1,2)) should be (sum(List(-1,2)))
    sumFold(List(16)) should be ( sum(List(16)))
    sumFold(List(16, -20)) should be ( sum(List(16, -20)))
  }


  "productFold" should "work the same as product" in {
    productFold(List()) should be (product(List()))
    productFold(Nil) should be (product(Nil))
    productFold(List(1.5,2)) should be (product(List(1.5,2)))
    productFold(List(-1,2)) should be (product(List(-1,2)))
    productFold(List(16)) should be (product(List(16)))
    productFold(List(16, -20)) should be ( product(List(16, -20)))
  }

  "lengthFoldRight" should "be 0 for empty lists" in {
    val l = List()
    assert(lengthFoldRight(l) == 0)
  }

  it should "be the actual length for bigger lists" in {
    assert(lengthFoldRight(List(1, 2, 5, 1)) == 4)
    assert(lengthFoldRight(List(6)) == 1)
  }
}

(new Ejercicios1Test).execute()




// COMMAND ----------

  //Ejercicios II
  //@annotation.tailrec

 def foldRight[A, B](as: List[A], z: B)(f: (A, B) => B): B = {
    as match {
      case Nil => z
      case h :: t => f(h, foldRight(t, z)(f))
    }
  }

  def foldLeft[A, B](as: List[A], z: B)(f: (B, A) => B): B = ???

  def sumFoldLeft(ints: List[Int]): Int = ???

  def productFoldLeft(ints: List[Double]): Double = ???

  def lengthFoldLeft[A](as: List[A]): Int = ???

def filter[A](l: List[A])(f: A => Boolean): List[A] = foldRight(l, List[A]())((elem, acc) => if (f(elem)) elem::acc else acc)

// COMMAND ----------

import org.scalatestplus.scalacheck._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by scouto.
  */
class Ejercicios2Test extends FlatSpec with Matchers with ScalaCheckPropertyChecks{


  "sumFoldLeft" should "work the same as sum" in {
    sumFoldLeft(List()) should be (sum(List()))
    sumFoldLeft(Nil) should be (sum(Nil))
    sumFoldLeft(List(1,2)) should be (sum(List(1,2)))
    sumFoldLeft(List(-1,2)) should be (sum(List(-1,2)))
    sumFoldLeft(List(16)) should be ( sum(List(16)))
    sumFoldLeft(List(16, -20)) should be ( sum(List(16, -20)))
  }


  "productFoldLeft" should "work the same as product" in {

    productFoldLeft(List()) should be (product(List()))
    productFoldLeft(Nil) should be (product(Nil))
    productFoldLeft(List(1.5,2)) should be (product(List(1.5,2)))
    productFoldLeft(List(-1,2)) should be (product(List(-1,2)))
    productFoldLeft(List(16)) should be (product(List(16)))
    productFoldLeft(List(16, -20)) should be ( product(List(16, -20)))
  }


  "lengthFoldLeft" should "be the same as length" in {
    val l = List()
    assert(lengthFoldLeft(l) == l.length)
    assert(lengthFoldLeft(List(1, 2, 5, 1)) == List(1, 2, 5, 1).length)
    assert(lengthFoldLeft(List(6)) == List(6).length)
  }

 
  "filter" should "empty list for empty lists" in {
    val l = List()

    assert(filter(l)((x: Int) => x == 0) == l)
  }

  it should "filter the elements" in {
    val l = List(1,2,3)

    assert(filter(l)(x => x %2 != 0) == List(1, 3))
    assert(filter(l)(x => x %2 == 0) == List(2))
  }

}


(new Ejercicios2Test).execute()