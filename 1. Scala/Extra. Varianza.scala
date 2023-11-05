// Databricks notebook source
//Contravarianza

abstract class Printer[-A] {
  def print(value: A): Unit
}

//Sabe como imprimir todos los animales (incluidos gatos) => Printer[Animal] es subtipo de Printer[Gato]
class AnimalPrinter extends Printer[Animal] {
  def print(animal: Animal): Unit =
    println("The animal's name is: " + animal.nombre)
}

//Sabe como imprimir los gatos (pero no todos los animales => Printer[Gato] no es subtipo de Priter[Animal]
class CatPrinter extends Printer[Gato] {
  def print(gato: Gato): Unit =
    println("The cat's name is: " + gato.nombre)
}

object Contravarianza extends App {
  val gato: Gato = Gato("Felix")

  def printMyCat(printer: Printer[Gato]): Unit = {
    printer.print(gato)
  }

  val catPrinter: Printer[Gato] = new CatPrinter
  val animalPrinter: Printer[Animal] = new AnimalPrinter

  printMyCat(catPrinter)
  printMyCat(animalPrinter)
}

// COMMAND ----------

//Covarianza

trait Animal {
  def nombre: String
}
case class Gato(nombre: String) extends Animal
case class Perro(nombre: String) extends Animal
case class Vaca(nombre: String) extends Animal

object Covarianza extends App {

  def imprimirNombres(animales: List[Animal]): Unit = {
    animales.foreach { animal =>
      println(animal.nombre)
    }
  }

  val gatos = List(Gato("Felix"), Gato("Isidoro"))
  val perros = List(Perro("Luigi"), Perro("Nanaki"))
  val vacas = List(Vaca("Rosalinda"), Vaca("Aurora"))


  //Funciona porque List es covariante => type List[+A]
  imprimirNombres(gatos)
  println()
  imprimirNombres(perros)
  println()
  imprimirNombres(vacas)

}