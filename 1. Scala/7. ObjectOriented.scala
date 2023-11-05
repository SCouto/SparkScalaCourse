// Databricks notebook source
class Person(val nombre: String, val age: Int)

object Person {
  def apply (nombre: String, age: Int) = new Person(nombre, age)
  def unapply(p: Person) = Some((p.nombre, p.age))
}

// COMMAND ----------

case class Person(nombre: String, age: Int)

// COMMAND ----------

//class Alumno(...

object Alumno {
  def apply (nombre: String, apellidos: String) = ???
  def unapply(a: Alumno) = ???
}

//class Asignatura (...

object Asignatura {
  def apply(nombre: String, plazas: Int, descripcion : Option[String] ) = ???

  def unapply(arg: oldAsignatura) = ???
}


// COMMAND ----------

sealed trait MyOption

class MyNone extends MyOption
class MySome(value: Int) extends MyOption 