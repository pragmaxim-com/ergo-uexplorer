package org.ergoplatform.uexplorer.mvstore.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.{BlockInfo, VersionedBlock}
import org.ergoplatform.uexplorer.mvstore.DbCodec
import org.ergoplatform.uexplorer.{Address, Height}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object AddressStatsCodec extends DbCodec[Address.Stats] {
  override def read(bytes: Array[Byte]): Address.Stats = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[Address.Stats])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def write(obj: Address.Stats): Array[Byte] = {
    val buffer = ByteBuffer.allocate(256)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, obj)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }
}
