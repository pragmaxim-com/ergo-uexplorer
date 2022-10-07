package org.ergoplatform.uexplorer.graphql

object ExampleData {

  case class Character(name: String, nicknames: List[String])

  case class CharacterArgs(name: String)

  val sampleCharacters: List[Character] = List(
    Character("James Holden", List("Jim", "Hoss")),
    Character("Naomi Nagata", Nil),
    Character("Amos Burton", Nil),
    Character("Alex Kamal", Nil),
    Character("Chrisjen Avasarala", Nil),
    Character("Josephus Miller", List("Joe")),
    Character("Roberta Draper", List("Bobbie", "Gunny"))
  )
}
