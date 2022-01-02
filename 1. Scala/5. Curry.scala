// Databricks notebook source
//Ejercicios I
def dropWhile[A](list: List[A], f: A => Boolean): List[A] = ???

def dropWhileNew[A](list: List[A])(f: A => Boolean): List[A] = ???

// COMMAND ----------

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by scouto.
  */
class CurryTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks{



  "dropWhile" should "be empty for empty lists" in {
    val l: List[Int] = List()
    assert(dropWhile(l, (x: Int) => x > 5) == Nil)
    assert(dropWhile(l, (x: Int) => x > 5) == Nil)
    assert(dropWhile(l, (x: Int) => x > 5) == Nil)
  }

  it should "remove elements as longer as the predicates satisfies" in {

    val lInt = List(1, 2, 5, 7)
    assert(dropWhile(lInt, (x: Int) => x < 5) == List(5, 7))
    assert(dropWhile(lInt, (x: Int) => x < 5) == List(5, 7))
    assert(dropWhile(lInt, (x: Int) => x < 10) == Nil)
    assert(dropWhile(lInt, (x: Int) => true) == Nil)
    assert(dropWhile(lInt, (x: Int) => false) == List(1, 2, 5, 7))

  }

  "dropWhileNew" should "be empty for empty lists" in {
    val l: List[Int] = List()
    assert(dropWhileNew(l)(x => x > 5) == Nil)
    assert(dropWhileNew(l)(_ > 5) == Nil)
    assert(dropWhileNew(l)(_ > 5) == Nil)
  }

  it should "remove elements as longer as the predicates satisfies" in {

    val lInt = List(1, 2, 5, 7)
    assert(dropWhileNew(lInt)(_ < 5) == List(5, 7))
    assert(dropWhileNew(lInt)( _ < 5) == List(5, 7))
    assert(dropWhileNew(lInt)(_ < 10) == Nil)
    assert(dropWhileNew(lInt)(x => true) == Nil)
    assert(dropWhileNew(lInt)(x => false) == List(1, 2, 5, 7))

  }
}

(new CurryTest).execute()

