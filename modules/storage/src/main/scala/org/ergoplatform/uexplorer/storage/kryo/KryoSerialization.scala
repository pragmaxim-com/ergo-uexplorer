package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.storage.kryo.*
import org.ergoplatform.uexplorer.mvstore.{DbCodec, MultiMapCodec}
import org.ergoplatform.uexplorer.{Address, BlockId, BoxId, Height, Value}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object KryoSerialization {

  object Implicits {
    implicit val blockIdsCodec: DbCodec[java.util.Set[BlockId]]              = BlockIdsCodec
    implicit val valueByBoxCodec: MultiMapCodec[java.util.Map, BoxId, Value] = ValueByBoxCodec
    implicit val blockMetadataCodec: DbCodec[BlockInfo]                      = BlockInfoCodec
    implicit val addressCodec: DbCodec[Address]                              = AddressCodec
  }

  val pool: Pool[Kryo] = new Pool[Kryo](true, false, 8) {
    protected def create: Kryo = {
      val kryo          = new Kryo()
      val mapSerializer = new MapSerializer()
      val setSerializer = new CollectionsSingletonSetSerializer()

      kryo.setRegistrationRequired(true)
      kryo.register(classOf[util.HashMap[_, _]], mapSerializer)
      kryo.register(classOf[util.HashSet[_]], setSerializer)
      kryo.register(classOf[BlockInfo])
      kryo.register(classOf[Address.Stats])
      setSerializer.setAcceptsNull(false)
      mapSerializer.setKeyClass(classOf[String], kryo.getSerializer(classOf[String]))
      mapSerializer.setKeysCanBeNull(false)
      mapSerializer.setValueClass(classOf[java.lang.Long], kryo.getSerializer(classOf[java.lang.Long]))
      mapSerializer.setValuesCanBeNull(false)
      kryo
    }
  }

}
