package models

object Session {
  def apply = new Session // from arguments
  def fromDBObject = new Session //TODO: from mongodb object
}

class Session {

}
