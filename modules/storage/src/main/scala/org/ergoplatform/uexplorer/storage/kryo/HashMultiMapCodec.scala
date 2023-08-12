package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.multimap.MultiMapCodec

import java.nio.ByteBuffer
import java.util
import scala.jdk.CollectionConverters.*
import scala.language.unsafeNulls

class HashMultiMapCodec[K, V] extends MultiMapCodec[java.util.Map, K, V] {

  override def readOne(key: K, valueByBoxId: java.util.Map[K, V]): Option[V] =
    Option(valueByBoxId.get(key))

  override def readAll(bytes: Array[Byte]): java.util.Map[K, V] = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[util.HashMap[K, V]])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def readPartially(only: IterableOnce[K])(
    existingOpt: Option[java.util.Map[K, V]]
  ): Option[java.util.Map[K, V]] =
    existingOpt.map { existingMap =>
      val partialResult = new util.HashMap[K, V]()
      only.iterator.foreach { k =>
        partialResult.put(k, existingMap.get(k))
      }
      partialResult
    }

  override def writeAll(valueByBoxId: java.util.Map[K, V]): Array[Byte] = {
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

  override def append(newValueByBoxId: IterableOnce[(K, V)])(
    existingOpt: Option[java.util.Map[K, V]]
  ): (Appended, java.util.Map[K, V]) =
    existingOpt.fold(true -> javaMapOf(newValueByBoxId)) { existingMap =>
      newValueByBoxId.iterator.forall { e =>
        val replaced: V | Null = existingMap.put(e._1, e._2)
        replaced == null
      } -> existingMap
    }

}
