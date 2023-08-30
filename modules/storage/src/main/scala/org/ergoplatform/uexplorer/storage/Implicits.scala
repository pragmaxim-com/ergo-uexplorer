package org.ergoplatform.uexplorer.storage

import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.{BoxId, *}
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.multimap.MultiMapCodec
import org.ergoplatform.uexplorer.mvstore.multiset.MultiSetCodec
import org.ergoplatform.uexplorer.storage.kryo.*

object Implicits {
  implicit def valueByBoxCodec[V]: MultiMapCodec[java.util.Map, BoxId, V]      = new HashMultiMapCodec[BoxId, V]
  implicit def amountByTokenCodec[V]: MultiMapCodec[java.util.Map, TokenId, V] = new HashMultiMapCodec[TokenId, V]

  implicit val blockIdsCodec: ValueCodec[java.util.Set[BlockId]] = BlockIdsCodec
  implicit val boxCodec: MultiSetCodec[java.util.Set, BoxId]     = BoxCodec
  implicit val blockCodec: ValueCodec[Block]                     = BlockCodec
  implicit val counterCodec: ValueCodec[SuperNodeCounter]        = CounterCodec
  implicit val addressCodec: ValueCodec[ErgoTreeHex]             = ErgoTreeHexCodec

  implicit val hotBoxCodec: HotKeyCodec[BoxId] = new HotKeyCodec[BoxId] {
    def serialize(key: BoxId): String   = key.unwrapped
    def deserialize(key: String): BoxId = BoxId.castUnsafe(key)
  }

  implicit val hotHexCodec: HotKeyCodec[HexString] = new HotKeyCodec[HexString] {
    import org.ergoplatform.uexplorer.HexString.unwrapped
    def serialize(key: HexString): String = key.unwrapped

    def deserialize(key: String): HexString = HexString.fromStringUnsafe(key)
  }

}
