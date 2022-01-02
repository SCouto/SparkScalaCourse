// Databricks notebook source

  //Ejercicio II
  def rotate(list: List[Int], x: Int): List[Int] = ???

  def removeFirstElement[A](list: List[A], f: A => Boolean): List[A] = ???

}

// COMMAND ----------

//Ejercicio III Opcional


import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by scouto.
  */
class AdministracionTest extends FlatSpec with Matchers{
  val manjarin = new Alumno("Javier", "Manjarin")
  val claudio  = new Alumno(nombre = "Claudio", apellidos = "Barragan")
  val alfredo   = new Alumno(apellidos = "Santaelena", nombre = "Alfredo")
  val fran  = new Alumno("Fran", "González")

  val asignatura = new Asignatura(
    nombre = "curso scala",
    descripcion = Some("Curso impartido de forma presencial"),
    plazas = 3)

  "Administracion" should "permitir inscribirse si hay plazas" in {
    val adm = new Administracion(Map(asignatura -> List(manjarin, claudio)))
    val optAdm = adm.alta(alfredo, asignatura)

    optAdm.get.relacionAlumnos should be (Map(asignatura -> List(alfredo, manjarin, claudio)))
  }

  it should "rechazar la inscipcion si ya está inscrito" in {
    val adm = new Administracion(Map(asignatura -> List(manjarin, claudio)))

    adm.alta(manjarin, asignatura) should be (None)
  }

  it should "rechazar la inscipcion si no quedan plazas" in {
    val adm = new Administracion(Map(asignatura -> List(manjarin, claudio, alfredo)))

    adm.alta(fran, asignatura) should be (None)
  }

  it should "levantar un mensaje de error si el alumno no estaba inscrito" in {
    val adm = new Administracion(Map(asignatura -> List(manjarin, claudio, alfredo)))
    val result = adm.baja(fran, asignatura)

    result should be (Left("Alumno no inscrito"))
  }

  it should "permitir si el alumno esta presente" in {
    val adm = new Administracion(Map(asignatura -> List(manjarin, claudio, alfredo)))
    val result = adm.baja(manjarin, asignatura)

    result.right.get.relacionAlumnos(asignatura).size should be (2)
    result.right.get.relacionAlumnos should be (Map(asignatura -> List(claudio, alfredo)))
  }
}




// COMMAND ----------

//Ejercicio IV Opcional

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by scouto.
  */
class AdministracionTest extends FlatSpec with Matchers{
  val manjarin: AlumnoTrait = AlumnoRepetidor("Javier", "Manjarin")
  val claudio: AlumnoTrait  = AlumnoRepetidor(nombre = "Claudio", apellidos = "Barragan")
  val alfredo: AlumnoTrait   = AlumnoRepetidor(apellidos = "Santaelena", nombre = "Alfredo")
  val fran: AlumnoTrait  = AlumnoRepetidor("Fran", "González")
  val diego: AlumnoTrait  = AlumnoNuevo("Diego", "Tristan")
  val djalma: AlumnoTrait  = AlumnoNuevo("Djalminha", "Feitosa")
  val turu: AlumnoTrait  = AlumnoNuevo("turu", "Flores")

  val asignaturaScala: AsignaturaTrait = AsignaturaSinPrioridad(
    nombre = "curso scala",
    descripcion = Some("Curso impartido en Amaris"),
    plazas = 3)


  val asignaturaJava: AsignaturaTrait = AsignaturaConPrioridad(
    nombre = "curso java",
    descripcion = Some("Curso impartido en Amaris"),
    plazas = 2)

  "Administracion trait" should "permitir inscribirse un repetidor si hay plazas en una asignatura sin prioridad" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(manjarin, claudio), asignaturaJava -> List()))

    adm.alta(alfredo, asignaturaScala).get.relacionAlumnos should be (Map(asignaturaScala -> List(alfredo, manjarin, claudio),  asignaturaJava -> List()))

  }

  it should "permitir inscribirse un repetidor si hay plazas en una asignatura con prioridad" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(manjarin, claudio), asignaturaJava -> List()))

    adm.alta(alfredo, asignaturaJava).get.relacionAlumnos should be (Map(asignaturaScala -> List(manjarin, claudio),  asignaturaJava -> List(alfredo)))

  }

  it should "permitir inscribirse un alumno nuevo si hay plazas en una asignatura con prioridad" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(manjarin, claudio), asignaturaJava -> List()))

    adm.alta(diego, asignaturaJava).get.relacionAlumnos should be (Map(asignaturaScala -> List(manjarin, claudio),  asignaturaJava -> List(diego)))
  }

  it should "permitir inscribirse un alumno nuevo si hay plazas en una asignatura sin prioridad" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(manjarin, claudio), asignaturaJava -> List()))

    adm.alta(diego, asignaturaScala).get.relacionAlumnos should be (Map(asignaturaScala -> List(diego, manjarin, claudio),  asignaturaJava -> List()))

  }

  it should "permitir inscribirse un alumno nuevo si hay repetidor aunque no haya plazas en una asignatura con prioridad" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(alfredo), asignaturaJava -> List(fran, manjarin)))

    adm.alta(diego, asignaturaJava).get.relacionAlumnos should be (Map(asignaturaScala -> List(alfredo),  asignaturaJava -> List(diego, manjarin)))

  }

  it should "rechazar la inscripcion de un alumno nuevo si no hay repetidores ni plazas en una asignatura con prioridad" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(alfredo), asignaturaJava -> List(djalma, turu)))

    adm.alta(diego, asignaturaJava) should be (None)
  }

  it should "rechazar la inscipcion si ya está inscrito" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(manjarin, claudio), asignaturaJava -> List()))

    adm.alta(manjarin, asignaturaScala) should be (None)
  }

  it should "rechazar la inscipcion si no quedan plazas" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(manjarin, claudio, alfredo), asignaturaJava -> List(fran)))

    adm.alta(fran, asignaturaScala) should be (None)
  }

  it should "levantar un mensaje de error si el alumno no estaba inscrito" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(manjarin, claudio, alfredo)))
    val result = adm.baja(fran, asignaturaScala)

    result should be (Left("Alumno no inscrito"))
  }

  it should "permitir si el alumno esta presente" in {
    val adm = AdministracionTrait(Map(asignaturaScala -> List(manjarin, claudio, alfredo), asignaturaJava -> List(fran)))
    val result = adm.baja(manjarin, asignaturaScala)

    result.right.get.relacionAlumnos(asignaturaScala).size should be (2)
    result.right.get.relacionAlumnos should be (Map(asignaturaScala -> List(claudio, alfredo), asignaturaJava -> List(fran)))
  }
}