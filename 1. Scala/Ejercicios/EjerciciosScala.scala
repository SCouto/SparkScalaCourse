// Databricks notebook source
//Ejercicio 1

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

//Ejercicio 2
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

//Ejercicio 3
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

// MAGIC %md # The cell below provides implementation for both foldLeft and foldRight with List and Streams. 
// MAGIC 
// MAGIC You can use them in exercises 4, 5 8, and 9

// COMMAND ----------

@annotation.tailrec
def foldLeft[A, B](as: List[A], z: B)(f: (B, A) => B): B = {
 as match {
   case Nil => z
   case h::t => foldLeft(t, f(z, h))(f)
 }
}

def foldRight[A, B](ints: List[A], z: B)(f: (A, B) => B): B = {
 ints match {
   case h::t => f(h, foldRight(t, z)(f))
   case Nil => z
 }
}

def foldRight[A, B](s: Stream[A], z: => B)(f: (A, => B) => B): B = {
 s match {
   case Stream.Empty => z
   case h#::t => f(h, foldRight(t, z)(f))
 }
}

@annotation.tailrec
final def foldLeft[A, B](s: Stream[A], z: => B)(f: ( => B, A) => B): B = {
 s match {
   case Stream.Empty => z
   case h#::t => foldLeft(t, f(z, h))(f)
 }
}





// COMMAND ----------

//Ejercicio 4 - Reverse
def reverse[A](as: List[A]): List[A] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class ReverseTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

 "reverse" should "be the list itself for empty lists or one-element lists" in {
    assert(reverse(List()) == List())
    assert(reverse(List("hello")) == List("hello"))
  }

  it should "be the reversed list for bigger lists" in {
    assert(reverse(List(1,2,3,4,5)) == List(5,4,3,2,1))
  }

}

(new ReverseTest).execute()

// COMMAND ----------

//Ejercicio 5 - Map 
  def map[A, B](l: List[A])(f: A => B): List[B] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class MapFoldTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

 "map" should "empty list for empty lists" in {
    val l = List()
    //type is needed to infere type
    assert(map(l)((x: Any) => x.toString) == l)
  }

  it should "convert the list according the function" in {
    val l = List(1,2,3)

    assert(map(l)(x => x.toString) == List("1", "2", "3"))
    assert(map(l)(x => x +1) == List(2,3,4))
  }

}

(new MapFoldTest).execute()

// COMMAND ----------

//Ejercicio 6 - dropWhile
@annotation.tailrec
final def dropWhile[A](s: Stream[A])(f: A => Boolean): Stream[A] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class DropWhileTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {
  
val emptyIntStream: Stream[Int] = Stream()  
  
  "dropWhile" should "be empty for empty streams" in {
    dropWhile(emptyIntStream)(_ > 5) should be(Stream())
  }

  it should "remove elements as longer as the predicates satisfies" in {

    dropWhile(Stream(1,2,3))(_ < 3).toList should be (List(3))
    dropWhile(Stream(1,2,3))(_ %2 != 0).toList should be (List(2,3))
    dropWhile(Stream(1,2,3))(_ < 10) should be (Stream())
    dropWhile(Stream(1,2,3))(x => true) should be (Stream())
    dropWhile(Stream(1,2,3))(x => false).toList should be (List(1, 2, 3))

  }
  
  
}

(new DropWhileTest).execute()

// COMMAND ----------

//Ejercicio 7 - take

def take[A](s: Stream[A], n: Int): Stream[A] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class TakeTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {
  
  
 "take" should "be empty for empty streams" in {
    take(Stream(), 5) should be (Stream())
    take(Stream(), 0) should be (Stream())
    take(Stream(), -5) should be (Stream())
  }

  it should "be empty when parameter is 0 or below 0" in {
    take(Stream(), 0) should be (Stream())
    take(Stream(1,2,3), 0) should be (Stream())
    take(Stream("Hello"), 0) should be (Stream())

    take(Stream(), -5) should be (Stream())
    take(Stream(1,2,3), -5) should be (Stream())
    take(Stream("Hello"), -5) should be (Stream())
  }


  it should "be the stream with the x elements at the beginning for bigger streams" in {
    take(Stream(1,2,3), 2).toList should be (List(1,2))
    take(Stream(1,2,3), 4).toList should be (List(1,2,3))
    take(Stream(1,2,3), 1).toList should be (List(1))
  }
}

(new TakeTest).execute()

// COMMAND ----------

//Ejercicio 8 - map fold
def map[A, B](s: Stream[A])(f: A => B): Stream[B] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class MapFoldStreamTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

 "map" should "empty list for empty lists" in {

    //type is needed to infere type
    assert(map(Stream())((x: Any) => x.toString).toList == List())
  }

  it should "convert the stream according the function" in {

    assert(map(Stream(1,2,3))(x => x.toString).toList == List("1", "2", "3"))
    assert(map(Stream(1,2,3))(x => x +1).toList == List(2,3,4))
  }
}

(new MapFoldStreamTest).execute()

// COMMAND ----------

//Ejercicio 9 -filter
def filter[A](s: Stream[A])(p: A => Boolean): Stream[A] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class FilterTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

  val emptyIntStream: Stream[Int] = Stream()  


  
 "filter" should "empty stream for empty stream" in {
    assert(filter(emptyIntStream)(x => x == 0) == emptyIntStream)
  }

  it should "filter the elements" in {
    assert(filter(Stream(1,2,3))(_ % 2 != 0).toList == Stream(1,3).toList)
    assert(filter(Stream(1,2,3))(_ % 2 == 0).toList == Stream(2).toList)
  }
}

(new FilterTest).execute()

// COMMAND ----------

//Ejercicio 10
def fibs: Stream[Int] = ???

// COMMAND ----------

fibs.take(10).toList
