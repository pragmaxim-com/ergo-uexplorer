package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.BlockInfo
import org.ergoplatform.uexplorer.mvstore.ValueCodec
import org.ergoplatform.uexplorer.{ErgoTreeHex, Height}

import java.nio.ByteBuffer
import java.util
import scala.util.Try

object ErgoTreeHexCodec extends ValueCodec[ErgoTreeHex] {
  override def readAll(bytes: Array[Byte]): ErgoTreeHex =
    new String(bytes).asInstanceOf[ErgoTreeHex] // do not call Address.fromStringUnsafe as it has been already validated

  override def writeAll(obj: ErgoTreeHex): Array[Byte] = obj.asInstanceOf[String].getBytes("UTF-8")
}
