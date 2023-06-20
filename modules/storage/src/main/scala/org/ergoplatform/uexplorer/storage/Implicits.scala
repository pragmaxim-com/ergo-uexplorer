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
  implicit val addressCodec: ValueCodec[Address]                           = AddressCodec

  implicit val superNodeAddressCodec: HotKeyCodec[Address] = new HotKeyCodec[Address] {
    import Address.unwrapped
    def serialize(key: Address): String = key.unwrapped

    // do not call Address.fromStringUnsafe as it has been already validated in BlockBuilder
    def deserialize(key: String): Address = key.asInstanceOf[Address]
  }

}
