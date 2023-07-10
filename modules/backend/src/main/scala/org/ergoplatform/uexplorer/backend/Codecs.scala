package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.HexString.unwrapped
import org.ergoplatform.uexplorer.Address.unwrappedAddress
import io.getquill.MappedEncoding
import org.ergoplatform.uexplorer.backend.boxes.*
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, ErgoTreeHash, ErgoTreeT8Hash, HexString, TxId}
import zio.json.*
import org.ergoplatform.uexplorer.db.Block

trait Codecs {
  given MappedEncoding[HexString, String] = MappedEncoding[HexString, String](_.unwrapped)
  given MappedEncoding[String, HexString] = MappedEncoding[String, HexString](HexString.castUnsafe)
  given MappedEncoding[Address, String]   = MappedEncoding[Address, String](_.unwrappedAddress)
  given MappedEncoding[String, Address]   = MappedEncoding[String, Address](Address.castUnsafe)
  given MappedEncoding[BoxId, String]     = MappedEncoding[BoxId, String](_.unwrapped)
  given MappedEncoding[String, BoxId]     = MappedEncoding[String, BoxId](BoxId.castUnsafe)
  given MappedEncoding[TxId, String]      = MappedEncoding[TxId, String](_.unwrapped)
  given MappedEncoding[String, TxId]      = MappedEncoding[String, TxId](TxId.castUnsafe)

  // json
  given JsonEncoder[HexString] = JsonEncoder[String].contramap(_.unwrapped)
  given JsonDecoder[HexString] = JsonDecoder[String].map(HexString.castUnsafe)
  given JsonEncoder[Address]   = JsonEncoder[String].contramap(_.unwrappedAddress)
  given JsonDecoder[Address]   = JsonDecoder[String].map(Address.castUnsafe)
  given JsonEncoder[BoxId]     = JsonEncoder[String].contramap(_.unwrapped)
  given JsonDecoder[BoxId]     = JsonDecoder[String].map(BoxId(_))
  given JsonEncoder[TxId]      = JsonEncoder[String].contramap(_.unwrapped)
  given JsonDecoder[TxId]      = JsonDecoder[String].map(TxId(_))
  given JsonDecoder[Utxo]      = DeriveJsonDecoder.gen[Utxo]
  given JsonDecoder[Box]       = DeriveJsonDecoder.gen[Box]
  given JsonEncoder[Utxo]      = DeriveJsonEncoder.gen[Utxo]
  given JsonEncoder[Box]       = DeriveJsonEncoder.gen[Box]
  given JsonEncoder[Block]     = DeriveJsonEncoder.gen[Block]
  given JsonDecoder[Block]     = DeriveJsonDecoder.gen[Block]

}
