// Databricks notebook source
//Ejercicio 1 . Mapas
  val romanos = Map(1 -> "I", 2 -> "II", 3 -> "III", 4 -> "IV", 5 -> "V", 6 -> "VI", 7 -> "VII", 8 -> "VIII", 9 -> "IX", 10 → "X")

  def printMap(myMap: Map[Int, String]) = ???

  def printSortedMap(myMap: Map[Int, String]) = ???


// COMMAND ----------

//utilities
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

//Ejercicio 2 - Reverse
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

//Ejercicio 3 - Map patternmatching
  def map[A, B](l: List[A])(f: A => B): List[B] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class MapPMTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

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

(new MapPMTest).execute()

// COMMAND ----------

//Ejercicio 4 - dropWhile
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

//Ejercicio 5 - take

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

//Ejercicio 6 - Exists
 def existsFold[A](s: Stream[A])(f: A => Boolean): Boolean = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class ExistsTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {
  
  val emptyIntStream: Stream[Int] = Stream()  

  
 "existsFold" should "return false for empty streams" in {
    existsFold(emptyIntStream)( _ > 5) should be (false)
  }

  it should "return true when the predicate if true for any value" in {
    existsFold(Stream(1,2,3))(_ == 1) should be (true)
    existsFold(Stream(1,2,3))(_ == 2) should be (true)
    existsFold(Stream(1,2,3))(_ == 3) should be (true)
  }

  it should "return false when the predicate if false for whichever value" in {
    existsFold(Stream(1,2,3))(_ == 4) should be (false)
    existsFold(Stream(1,2,3))(_ < 0) should be (false)
  }
}

(new ExistsTest).execute()

// COMMAND ----------

//Ejercicio 7 - map fold
def map[A, B](s: Stream[A])(f: A => B): Stream[B] = ???

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class MapFoldTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

 "map" should "empty list for empty lists" in {

    //type is needed to infere type
    assert(map(Stream())((x: Any) => x.toString).toList == List())
  }

  it should "convert the stream according the function" in {

    assert(map(Stream(1,2,3))(x => x.toString).toList == List("1", "2", "3"))
    assert(map(Stream(1,2,3))(x => x +1).toList == List(2,3,4))
  }
}

(new MapFoldTest).execute()

// COMMAND ----------

//Ejercicio 8 -filter
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