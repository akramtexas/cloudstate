package io.cloudstate.proxy.broker

import spray.json.DefaultJsonProtocol._
import spray.json._

case class ShoppingItem(userID: String, productId: String)

object ShoppingCartJson {
  implicit val shoppingFormat: JsonFormat[ShoppingItem] = jsonFormat2(ShoppingItem)
}
