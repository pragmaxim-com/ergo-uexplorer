package org.ergoplatform.uexplorer.storage

import org.ergoplatform.uexplorer.*
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.Counter
import org.ergoplatform.uexplorer.storage.kryo.*

object Implicits {
  implicit val blockIdsCodec: ValueCodec[java.util.Set[BlockId]]           = BlockIdsCodec
  implicit val valueByBoxCodec: MultiMapCodec[java.util.Map, BoxId, Value] = ValueByBoxCodec
  implicit val blockInfoCodec: ValueCodec[BlockInfo]                       = BlockInfoCodec
  implicit val counterCodec: ValueCodec[Counter]                           = CounterCodec
  implicit val addressCodec: ValueCodec[ErgoTreeHex]                       = ErgoTreeHexCodec

  implicit val superNodeErgoTreeCodec: HotKeyCodec[ErgoTreeHex] = new HotKeyCodec[ErgoTreeHex] {
    import ErgoTreeHex.unwrapped
    def serialize(key: ErgoTreeHex): String = key.unwrapped

    // do not call Address.fromStringUnsafe as it has been already validated in BlockBuilder
    def deserialize(key: String): ErgoTreeHex = key.asInstanceOf[ErgoTreeHex]
  }

}
