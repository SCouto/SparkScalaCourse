// Databricks notebook source
  //Ejercicios I Pattern MAtching
  def headOption[A](s: Stream[A]): Option[A] = ???

  def toList[A](s: Stream[A]): List[A] = ???

  //@annotation.tailrec
  final def drop[A](s: Stream[A], n: Int): Stream[A] = ???

  def takeWhile[A](s: Stream[A])(p: A => Boolean): Stream[A] = ???

// COMMAND ----------

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by couto .
  */
class Ejercicios1Test extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

    val stream = Stream(1,2,3)
    val size1Stream = Stream("Hello")
    val emptyStream = Stream()
    val emptyIntStream: Stream[Int] = Stream()

  "headOption" should "return None for empty Streams" in {
      headOption(emptyStream) should be (None)
  }

  it should "return Some(headValue) for empty Streams" in {
    headOption(stream) should be (Some(1))
  }

  "toList" should "return Empty List for empty Streams" in {
    toList(emptyStream) should be (List())
    toList(emptyStream) should be (Nil)
  }

  it should "return the list with the same values for non-empty Streams" in {
    toList(stream) should be (List(1,2,3))
  }


  "drop" should "be empty for empty streams" in {
    drop(emptyStream, 5) should be (Stream())
    drop(emptyStream, 0) should be (Stream())
    drop(emptyStream, -5) should be (Stream())
  }

  it should "be empty for one-element streams" in {
    drop(size1Stream, 5) should be (Stream())
  }

  it should "be the same stream for 0 or below 0 drops regardless the stream itself" in {
    drop(emptyStream, 0) should be (emptyStream)
    drop(stream, 0) should be (stream)
    drop(size1Stream, 0) should be (size1Stream)

    drop(emptyStream, -5) should be (emptyStream)
    drop(stream, -1) should be (stream)
    drop(size1Stream, -2) should be (size1Stream)
  }

  it should "be the stream without the x elements at the beginning for bigger streams" in {
   drop(stream, 2).toList should be (List(3))
   drop(stream, 4) should be (Stream())
   drop(stream, 1).toList should be (List(2,3))
   drop(stream, 15) should be (Stream())

  }

  "takeWhile" should "be empty for empty streams" in {
    takeWhile(emptyIntStream)(_ > 5) should be (Stream())
  }

  it should "take elements as longer as the predicates satisfies" in {

    takeWhile(stream)(_ < 3).toList should be (List(1,2))
    takeWhile(stream)(_ %2 != 0).toList should be (List(1))
    takeWhile(stream)(_ < 10).toList should be (List(1, 2, 3))
    takeWhile(stream)(_ < -10) should be (Stream())
    takeWhile(stream)(x => true).toList should be (List(1, 2, 3))
    takeWhile(stream)(x => false) should be (Stream())

  }

}

(new Ejercicios1Test).execute()

// COMMAND ----------

  //Ejercicios II Fold
 //Ejercicios II
  def foldRight[A, B](s: Stream[A], z: => B)(f: (A, => B) => B): B = ???

  @annotation.tailrec
  final def foldLeft[A, B](s: Stream[A], z: => B)(f: ( => B, A) => B): B = ???

  def forAllFold[A](s: Stream[A])(p:A => Boolean): Boolean = ???
 
  def existsFoldRight[A](s: Stream[A])(f: A => Boolean): Boolean = ???

  def existsFold[A](s: Stream[A])(f: A => Boolean): Boolean = ???

  def headOptionFold[A](s: Stream[A]): Option[A] = ???

// COMMAND ----------

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by couto .
  */
class Ejercicios2Test extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

    val stream = Stream(1,2,3)
    val size1Stream = Stream("Hello")
    val emptyStream = Stream()
    val emptyIntStream: Stream[Int] = Stream()


  "forAllFold" should "return true for empty streams" in {
    forAllFold(emptyIntStream)( _ > 5) should be (true)
  }
  
  it should "return true or false if all the elmnts matches" in {
    forAllFold(stream)( _ < 5) should be (true)
    forAllFold(stream)( _ > 2) should be (false)
  }

  "exists" should "return false for empty streams" in {
    existsFoldRight(emptyIntStream)( _ > 5) should be (false)
    existsFold(emptyIntStream)( _ > 5) should be (false)
  }

  it should "return true when the predicate if true for any value" in {
    existsFoldRight(stream)(_ == 1) should be (true)
    existsFoldRight(stream)(_ == 2) should be (true)
    existsFoldRight(stream)(_ == 3) should be (true)
    existsFold(stream)(_ == 1) should be (true)
    existsFold(stream)(_ == 2) should be (true)
    existsFold(stream)(_ == 3) should be (true)    
  }

  it should "return false when the predicate if false for whichever value" in {
    existsFoldRight(stream)(_ == 4) should be (false)
    existsFoldRight(stream)(_ < 0) should be (false)
    existsFold(stream)(_ == 4) should be (false)
    existsFold(stream)(_ < 0) should be (false)    
  }
  
  
  "headOptionFold" should "return None for empty streams" in {
    headOptionFold(emptyIntStream) should be (None)
  }
  
  it should "return the head in an Option if found" in {
    headOptionFold(stream) should be (Some(1))
  }

}


(new Ejercicios2Test).execute()

// COMMAND ----------

//Infinitos
val ones: Stream[Int] = 1#::ones

//Ejercicios III
def constant[A](a: A): Stream[A] = ???

def from(n: Int): Stream[Int] = ???

// COMMAND ----------

//Ejercicio IV
def unfold[A,S](z: S)(f: S => Option[(A,S)]): Stream[A] = ???

def onesUnfold: Stream[Int] = ???

def constantUnfold[A](a: A): Stream[A] = ???

def fromUnfold(n: Int): Stream[Int] = ???