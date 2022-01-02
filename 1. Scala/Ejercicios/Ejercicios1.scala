// Databricks notebook source
//Ejercicio 1
def applyRateEither(cant: Option[Double], tipo: Option[Double]): Either[String, Double] =  ???

// COMMAND ----------

//Ejercicio 2
  def applyRateEither(cant: Either[String, Double], tipo: Either[String, Double]): Either[String, Double] =  ???

// COMMAND ----------

//Ejercicio 3

def penultimate(list: List[Int]): Option[Int] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class PenultimateTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

  "penultimate" should "return the penultimate element for each list " in {
    penultimate(List(1,2,3)) should be (Some(2))
    penultimate(List(1,2)) should be (Some(1))
    penultimate(List(1,2,3, 4, 5, 6)) should be (Some(5))
  }
  
  it should "return None if there is no penultimate element" in {
    penultimate(List()) should be (None)
    penultimate(List(5)) should be (None)
  }

}


(new PenultimateTest).execute()

// COMMAND ----------

//Ejercicio 4 
def duplicate(list: List[Int], k: Int): List[Int] =  ???



// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class DuplicateTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

  "duplicate" should "return empty list for empty lists" in {
     duplicate(List(), 4) should be (List())
  }
  
  it should "return empty list if the k parameter is negative" in {
     duplicate(List(), -4) should be (List())
     duplicate(List(1,2), -4) should be (List())
  }
  
  it should "return duplicated list" in {
     duplicate(List(1,5), 2) should be (List(1,1,5,5))
  }
}
(new DuplicateTest).execute()

// COMMAND ----------

//Ejercicio 5
def nth(list: List[Int], n: Int): Option[Int] = ???



// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class NthTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

  "nth" should "return None for empty lists" in {
     nth(List(), 2) should be (None)
     nth(List(), -2) should be (None)
  }
  
  it should "return None if the n parameter is abode the list size" in {
     nth(List(1,2), 5) should be (None)
  }
  
  it should "return None if the n parameter is negative" in {
     nth(List(), -4) should be (None)
     nth(List(1,2), -4) should be (None)
  }
  
  it should "return desired element if it exists list" in {
     nth(List(1,5), 2) should be (Some(5))
  }
}
(new NthTest).execute()

// COMMAND ----------

//Ejercicio 6
def isPalindrome(word: String): Boolean = ???


// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class PalindromeTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

  "isPalindrome" should "return true if the expression is a palindrome no matter the case" in {
     isPalindrome("sopapos") should be (true)
     isPalindrome("SoPapOs") should be (true)
     isPalindrome("Sopapos") should be (true)
  }
  
  it should "return true if the expression is a palindrome (BLANKSPACES CHECK)" in {
     isPalindrome("a torre da derrota") should be (true)
  }
  
    it should "return false if the expression is not a palindrome" in {
     isPalindrome("Escuela de organizacion industrial") should be (false)
  }
    
}
(new PalindromeTest).execute()

// COMMAND ----------

//Ejercicio 7
def uncurry(f: Int => Int => Int): (Int, Int) => Int = ???

def curry(f: (Int, Int) => Int): Int => Int => Int = ???

def genUncurry[A,B,C](f: A => B => C): (A, B) => C = ???

def genCurry[A,B,C](f: (A, B) => C): A => B => C = ???
