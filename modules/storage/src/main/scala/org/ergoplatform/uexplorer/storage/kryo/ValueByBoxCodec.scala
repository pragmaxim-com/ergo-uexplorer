package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.multimap.MultiMapCodec
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHex, Height}

import java.nio.ByteBuffer
import java.util
import scala.jdk.CollectionConverters.*
import scala.language.unsafeNulls
import scala.util.Try

class ValueByBoxCodec[V] extends MultiMapCodec[java.util.Map, BoxId, V] {

  override def readOne(key: BoxId, valueByBoxId: java.util.Map[BoxId, V]): Option[V] =
    Option(valueByBoxId.get(key))

  override def readAll(bytes: Array[Byte]): java.util.Map[BoxId, V] = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[util.HashMap[BoxId, V]])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def readPartially(only: IterableOnce[BoxId])(
    existingOpt: Option[java.util.Map[BoxId, V]]
  ): Option[java.util.Map[BoxId, V]] =
    existingOpt.map { existingMap =>
      val partialResult = new util.HashMap[BoxId, V]()
      only.iterator.foreach { k =>
        partialResult.put(k, existingMap.get(k))
      }
      partialResult
    }

  override def writeAll(valueByBoxId: java.util.Map[BoxId, V]): Array[Byte] = {
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

  override def append(newValueByBoxId: IterableOnce[(BoxId, V)])(
    existingOpt: Option[java.util.Map[BoxId, V]]
  ): (Appended, java.util.Map[BoxId, V]) =
    existingOpt.fold(true -> javaMapOf(newValueByBoxId)) { existingMap =>
      newValueByBoxId.iterator.forall { e =>
        val replaced: V | Null = existingMap.put(e._1, e._2)
        replaced == null
      } -> existingMap
    }

}
