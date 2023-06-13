package org.ergoplatform.uexplorer.utxo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.{Address, BlockMetadata, Height}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

trait KryoSerialization extends UtxoSerialization with BlockIdsSerialization {

  protected def deserStats(bytes: Array[Byte]): Address.Stats = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[Address.Stats])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  protected def deserBlock(bytes: Array[Byte]): BlockMetadata = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[BlockMetadata])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  protected def serStats(stats: Address.Stats): Array[Byte] = {
    val buffer = ByteBuffer.allocate(256)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, stats)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }

  protected def serBlock(block: BlockMetadata): Try[Array[Byte]] = Try {
    val buffer = ByteBuffer.allocate(4096)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, block)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }
}

object KryoSerialization {

  val pool: Pool[Kryo] = new Pool[Kryo](true, false, 8) {
    protected def create: Kryo = {
      val kryo          = new Kryo()
      val mapSerializer = new MapSerializer()
      val setSerializer = new CollectionsSingletonSetSerializer()

      kryo.setRegistrationRequired(true)
      kryo.register(classOf[util.HashMap[_, _]], mapSerializer)
      kryo.register(classOf[util.HashSet[_]], setSerializer)
      kryo.register(classOf[BlockInfo])
      kryo.register(classOf[BlockMetadata])
      kryo.register(classOf[Address.Stats])
      setSerializer.setAcceptsNull(false)
      mapSerializer.setKeyClass(classOf[String], kryo.getSerializer(classOf[String]))
      mapSerializer.setKeysCanBeNull(false)
      mapSerializer.setValueClass(classOf[java.lang.Long], kryo.getSerializer(classOf[java.lang.Long]))
      mapSerializer.setValuesCanBeNull(false)
      kryo
    }
  }

}
