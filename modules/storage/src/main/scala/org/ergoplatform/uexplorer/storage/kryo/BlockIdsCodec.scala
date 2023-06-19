package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.mvstore.ValueCodec
import org.ergoplatform.uexplorer.{Address, BlockId, Height}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object BlockIdsCodec extends ValueCodec[java.util.Set[BlockId]] {
  override def readAll(bytes: Array[Byte]): java.util.Set[BlockId] = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[util.HashSet[BlockId]])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def writeAll(blockIds: java.util.Set[BlockId]): Array[Byte] = {
    val buffer = ByteBuffer.allocate((blockIds.size() * 64) + 32)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, blockIds)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }
}
