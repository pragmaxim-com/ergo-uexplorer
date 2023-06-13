package org.ergoplatform.uexplorer.utxo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.{BlockInfo, DbCodec}
import org.ergoplatform.uexplorer.{Address, BlockMetadata, Height}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object AddressCodec extends DbCodec[Address] {
  override def read(bytes: Array[Byte]): Address = Address.fromStringUnsafe(new String(bytes))

  override def write(obj: Address): Array[Byte] = obj.asInstanceOf[String].getBytes("UTF-8")
}
