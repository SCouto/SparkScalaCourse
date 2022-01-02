// Databricks notebook source
def myIf[A](onCond: Boolean, onTrue: => A, onFalse: =>A): A = {
   if (onCond)
     onTrue
   else
     onFalse
}


def myIf2[A](onCond: Boolean, onTrue: A, onFalse: A): A = {
   if (onCond)
     onTrue
   else
     onFalse
}


println("if1")
myIf(true, println("hello"), println("bye"))

println("if2")
myIf2(true, println("hello"), println("bye"))




// COMMAND ----------

def duplicateStrict(x: Double): Double = {
    println(x*2)
    x*2
  }

  def duplicateNonStrict(x: => Double): Double = {
    println(x*2)
    x*2
  }


  def duplicateNonStrictLazy(x: => Double): Double = {
    lazy val j = x
    println(j+j)
    j+j
  }

  duplicateStrict({println("duplicateStrict"); 10+11})
  println()
  duplicateNonStrict({println("duplicateNonStrict"); 10+11})
  println()
  duplicateNonStrictLazy({println("duplicateNonStrictLazy"); 10+11})