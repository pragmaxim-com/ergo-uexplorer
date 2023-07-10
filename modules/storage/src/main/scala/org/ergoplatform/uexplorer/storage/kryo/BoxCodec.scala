package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.multiset.MultiSetCodec
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHex, Height}

import java.nio.ByteBuffer
import java.util
import scala.jdk.CollectionConverters.*
import scala.language.unsafeNulls
import scala.util.Try

object BoxCodec extends MultiSetCodec[java.util.Set, BoxId] {

  override def readAll(bytes: Array[Byte]): java.util.Set[BoxId] = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[util.HashSet[BoxId]])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def writeAll(values: java.util.Set[BoxId]): Array[Byte] = {
    val buffer = ByteBuffer.allocate((values.size() * 70) + 512)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, values)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }

  override def append(values: IterableOnce[BoxId])(
    existingOpt: Option[java.util.Set[BoxId]]
  ): (Appended, java.util.Set[BoxId]) =
    existingOpt.fold(true -> javaSetOf(values)) { existingSet =>
      values.iterator.forall { v =>
        existingSet.add(v)
      } -> existingSet
    }

}
