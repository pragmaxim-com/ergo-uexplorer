package org.ergoplatform.uexplorer.mvstore.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.mvstore.DbCodec
import org.ergoplatform.uexplorer.{Address, Height}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object AddressCodec extends DbCodec[Address] {
  override def readAll(bytes: Array[Byte]): Address =
    new String(bytes).asInstanceOf[Address] // do not call Address.fromStringUnsafe as it has been already validated

  override def writeAll(obj: Address): Array[Byte] = obj.asInstanceOf[String].getBytes("UTF-8")
}
