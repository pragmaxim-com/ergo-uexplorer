package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.Counter
import org.ergoplatform.uexplorer.mvstore.ValueCodec
import org.ergoplatform.uexplorer.{Address, Height}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object CounterCodec extends ValueCodec[Counter] {
  override def readAll(bytes: Array[Byte]): Counter = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[Counter])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def writeAll(obj: Counter): Array[Byte] = {
    val buffer = ByteBuffer.allocate(64)
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
