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

object BlockMetadataCodec extends DbCodec[VersionedBlock] {
  override def read(bytes: Array[Byte]): VersionedBlock = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[VersionedBlock])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def write(obj: VersionedBlock): Array[Byte] = {
    val buffer = ByteBuffer.allocate(4096)
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
