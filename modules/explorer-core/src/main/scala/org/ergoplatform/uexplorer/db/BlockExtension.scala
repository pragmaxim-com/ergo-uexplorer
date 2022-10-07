package org.ergoplatform.uexplorer.db

import io.circe.magnolia.derivation.decoder.semiauto.deriveMagnoliaDecoder
import io.circe.magnolia.derivation.encoder.semiauto.deriveMagnoliaEncoder
import io.circe.{Codec, Json}
import org.ergoplatform.uexplorer.{BlockId, HexString}

/** Represents `node_extensions` table.
  */
final case class BlockExtension(
  headerId: BlockId,
  digest: HexString,
  fields: Json // arbitrary key->value dictionary
)

object BlockExtension {

  implicit val codec: Codec[BlockExtension] = Codec.from(deriveMagnoliaDecoder, deriveMagnoliaEncoder)
}
