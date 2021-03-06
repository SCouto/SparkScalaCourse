// Databricks notebook source

import scala.util.{Failure, Success, Try}

  def divisionWithException (x: Int, y: Int) : Int = {
    try {
      x/y
    }catch {
      case e: Exception => println("division entre 0"); x
    }
  }

  def divisionwithTryPM (x: Int, y: Int) : Int = {

    Try(x/y) match {
      case Success(resultado) =>resultado
      case Failure(error) => println(error); x
    }
  }

  def divisionwithTry (x: Int, y: Int) : Try[Int] = {
    Try(x/y)
  }


  def divisionwithOption (x: Int, y: Int) : Option[Int] = {

    Try(x/y) match {
      case Success(resultado) => Some(resultado)
      case _ => None
    }
  }

  def divisionwithEither (x: Int, y: Int) : Either[String, Int] = {

    Try(x/y) match {
      case Success(resultado) => Right(resultado)
      case Failure(throwable) => Left(throwable.getMessage)
    }
  }

// COMMAND ----------

//Ejercicios
def applyRate(cant: Double, tipo: Option[Double]): Double = {
    cant * tipo.getOrElse(1.5)
}

def applyOptionalRate(cant: Option[Double], tipo: Option[Double]): Option[Double] = {
    //si no tienes cant => None
    //si no tienes tipo => cand * 1.5
    //si te falta algo => None  SE PUEDE HACER PATTERN MATCHINH DE TUPLAS

    (cant, tipo) match {
      case (Some(c), Some(t)) => Some(c * t)
      case _ => None
    }
}

def second(list: List[Int]): Option[Int] = {
  list match {
    case h :: h2 :: t => Some(h2)
    case _ => None
  }
}

// COMMAND ----------

import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck._


class EjerciciosTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {

  "applyRate" should "apply rate if it is given" in {
    applyRate(100, Some(2)) should be (200)
  }
  
  it should "apply default rate if it is not given" in {
    applyRate(100, None) should be (150)
  }
  

  "second" should "return the second element for each list " in {
    second(List(1,2,3)) should be (Some(2))
  }
  
  it should "return None if the second element is not present" in {
    second(List()) should be (None)
    second(List(5)) should be (None)
  }
 
}


(new EjerciciosTest).execute()