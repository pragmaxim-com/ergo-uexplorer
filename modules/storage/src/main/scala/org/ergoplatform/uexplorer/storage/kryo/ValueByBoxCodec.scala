package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.{Address, BoxId, Height, Value}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object ValueByBoxCodec extends MultiMapCodec[java.util.Map, BoxId, Value] {

  override def readOne(key: BoxId, valueByBoxId: java.util.Map[BoxId, Value]): Option[Value] =
    Option(valueByBoxId.get(key))

  override def readAll(bytes: Array[Byte]): java.util.Map[BoxId, Value] = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[util.HashMap[BoxId, Value]])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def writeAll(valueByBoxId: java.util.Map[BoxId, Value]): Array[Byte] = {
    val buffer = ByteBuffer.allocate((valueByBoxId.size() * 72) + 512)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, valueByBoxId)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }

  def append(newValueByBoxId: IterableOnce[(BoxId, Value)])(
    existingOpt: Option[java.util.Map[BoxId, Value]]
  ): (Appended, java.util.Map[BoxId, Value]) =
    existingOpt.fold(true -> javaMapOf(newValueByBoxId)) { existingMap =>
      val appended = newValueByBoxId.iterator.forall(e => Option(existingMap.put(e._1, e._2)).isEmpty)
      appended -> existingMap
    }

}
