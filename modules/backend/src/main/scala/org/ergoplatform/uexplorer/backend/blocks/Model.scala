package org.ergoplatform.uexplorer.backend.blocks

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class Info(lastHeight: Int)

object Info {
  implicit val encoder: JsonEncoder[Info] = DeriveJsonEncoder.gen[Info]
  implicit val decoder: JsonDecoder[Info] = DeriveJsonDecoder.gen[Info]
}
