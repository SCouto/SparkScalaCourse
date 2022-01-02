// Databricks notebook source
  //Recusividad
  def wrongFactorial(n: Int) = {
    if (n <= 0) 1
    else n * factorial(n - 1)
  }

  def factorial(n: Int) = {

    @annotation.tailrec
    def rec(acc: Int, current: Int): Int = {
      if (current <= 0) acc
      else rec(acc * current, current - 1)

    }

    rec(1, n)
  }


// COMMAND ----------

//Ejercicios I

//def operate

def max(list: List[Int]): Int = ???

def second(list: List[Int]): Int = ???

//@annotation.tailrec
def nth(list: List[Int], n: Int): Int = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class Ejercicios1Test extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

  "max" should "give the max for each list " in {
    max(List(1,2,3)) should be (3)
    max(List(-5,-26,-15)) should be (-5)
  }
  
   it should "return MinValue if the list is empty" in {
    max(List()) should be (Int.MinValue)
  }
  
  "second" should "return the second element for each list " in {
    second(List(1,2,3)) should be (2)
  }
  
  it should "return -1 if the second element is not present" in {
    second(List()) should be (-1)
    second(List(5)) should be (-1)
  }
  
  "nth" should "give the nth element for each list " in {
    nth(List(1,2,3), 1) should be (1)
    nth(List(3,34,75), 2) should be (34)
  }
  
    
  it should "return -1 if the required element is not present" in {
    nth(List(1,2,3), 5) should be (-1)
  }
  
      
  it should "work returning -1 with negative elements" in {
    nth(List(1,2,3), -1) should be (-1)
  }
}


(new Ejercicios1Test).execute()

// COMMAND ----------

//Ejercicios II
def addAtTheEnd(list: List[Int], elem: Int): List[Int] = ???

def addAtTheEndIfNotExists(list: List[Int], elem: Int): List[Int] = ???

def doubleIfOdd(list: List[Int]): List[Int] = ???



// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class Ejercicios2Test extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

  val genInteger = for (n <- Gen.choose(-500, 500)) yield n
  val genIntList = Gen.containerOf[List, Int](genInteger)
  

  "addAtTheEnd" should "create a single element list for empty lists" in {
    addAtTheEnd(Nil,3) should be (List(3))
  }

  it should "add an element to the given list" in {

    forAll(genIntList, genInteger) { (myList, myInt) =>

      val result = addAtTheEnd(myList, myInt)
      myList.length +1 shouldEqual result.length
      result.last shouldEqual myInt
    }
  }
  
   "addAtTheEndIfNotExists" should "create a single elmnt list for empty lists" in {
    addAtTheEndIfNotExists(Nil,3) should be (List(3))
  }
  
  it should "leave the list as was if the elmnt exists" in {
    addAtTheEndIfNotExists(List(3,4),3) should be (List(3,4))
    addAtTheEndIfNotExists(List(3,4,5,6,7,8),8) should be (List(3,4,5,6,7,8))
  }
  
  it should "work as addAtTheEnd if the elmnt does not exists" in {
    addAtTheEndIfNotExists(List(3,4),2) should be (addAtTheEnd(List(3,4),2))
  }
  
  "doubleIfOdd" should "duplicate elements that are odd" in {
    doubleIfOdd(List(1,2,3,4,5,4)) should be (List(2,6,10))
  }
  
  
}


(new Ejercicios2Test).execute()

// COMMAND ----------

//Ejercicio III
val romanos = Map(1 -> "I", 2 -> "II", 3 -> "III", 4 -> "IV", 5 -> "V", 6 -> "VI", 7 -> "VII", 8 -> "VIII", 9 -> "IX", 10 → "X")

// COMMAND ----------

//Ejercicios IV
def sum(ints: List[Int]): Int = ???

def product(ints: List[Double]): Double = ???

def tail[A](list: List[A]): List[A] = ???

def setHead[A](list: List[A], newHead: A): List[A] = ???

def drop[A](list: List[A], n: Int): List[A] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._



class Ejercicios4Test extends FlatSpec with Matchers with ScalaCheckPropertyChecks {



  "sum" should "return 0 for empty listas" in {
    sum(List()) should be (0)
    sum(Nil) should be (0)
  }

  it should "return the addition" in {

    sum(List(1,2)) should be (3)
    sum(List(-1,2)) should be (1)
    sum(List(16)) should be (16)
    sum(List(16, -20)) should be (-4)
  }



  "tail" should "be empty for empty lists" in {
    val l = List()
    assert(tail(l) == Nil)
  }

  it should "be empty for one-element lists" in {
    val l = List(1)
    assert(tail(l) == Nil)
  }

  it should "be the tail itself for bigger lists" in {

    val lInt = List(1, 2, 5, 7)
    assert(tail(lInt) == List(2, 5, 7))

    val lString = List("myName", "mySurname")
    assert(tail(lString) == List("mySurname"))

    val lBool = List(true, true, false)
    assert(tail(lBool) == List(true, false))

  }

  "setHead" should "be one element list for empty lists" in {
    val l = List()
    setHead(l, 5) should be (List(5))
  }

  it should "be the same list with a new head" in {
    val l = List(7, 9, 10, 12)
    setHead(l, 5) should be (List(5, 9, 10, 12))
  }


  "drop" should "be empty for empty lists" in {
    val l = List()
    assert(drop(l, 5) == Nil)
    assert(drop(l, 0) == Nil)
    assert(drop(l, -5) == Nil)
  }

  it should "be empty for one-element lists" in {
    val l = List(1)
    assert(drop(l, 5) == Nil)
  }

  it should "be the same list for 0 drops regardless the list itself" in {
    val l = List(1)
    val ls = List("myName", "mySurname")
    assert(drop(l, 0) == l)
    assert(drop(Nil, 0) == Nil)
    assert(drop(ls, 0) == ls)
  }

  it should "be the list without the x elements at the beginning for bigger lists" in {

    val lInt = List(1, 2, 5, 7)
    assert(drop(lInt, 3) === List(7))
    assert(drop(lInt, 4) == Nil)
    assert(drop(lInt, 1) == tail(lInt))
    assert(drop(lInt, 15) == Nil)

  }

}

(new Ejercicios4Test).execute()