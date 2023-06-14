package org.ergoplatform.uexplorer.mvstore.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.mvstore.kryo.{AddressCodec, AddressStatsCodec, BlockIdsCodec, BlockMetadataCodec, ValueByBoxCodec}
import org.ergoplatform.uexplorer.mvstore.{DbCodec}
import org.ergoplatform.uexplorer.{Address, BlockId, BlockMetadata, BoxId, Height, Value}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object KryoSerialization {

  object Implicits {
    implicit val addressStatsCodec: DbCodec[Address.Stats]             = AddressStatsCodec
    implicit val blockIdsCodec: DbCodec[java.util.Set[BlockId]]        = BlockIdsCodec
    implicit val valueByBoxCodec: DbCodec[java.util.Map[BoxId, Value]] = ValueByBoxCodec
    implicit val blockMetadataCodec: DbCodec[BlockMetadata]            = BlockMetadataCodec
    implicit val addressCodec: DbCodec[Address]                        = AddressCodec

    def javaSetOf[T](e: T): java.util.Set[T] = {
      val set = new java.util.HashSet[T]()
      set.add(e)
      set
    }
    def javaMapOf[K, V](k: K, v: V): java.util.Map[K, V] = {
      val map = new java.util.HashMap[K, V]()
      map.put(k, v)
      map
    }
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
      kryo.register(classOf[BlockMetadata])
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
